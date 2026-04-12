use std::sync::Arc;
use std::time::{Duration, Instant};

use serenity::all::{
    self as serenity_model, CommandInteraction, Context, CreateMessage, EditMessage, EventHandler,
    Interaction as SerenityInteraction, Message as SerenityMessage, MessageId, Ready,
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::traits::{ChannelEvent, ChannelResponse, ChannelStatus};

use super::{DiscordFile, DiscordGroupFilter, DiscordOnMessageFn, commands, convert};

/// Serenity event handler that bridges Discord events into OpenCrust `ChannelEvent`s.
pub struct DiscordHandler {
    /// Broadcast sender for emitting channel events to subscribers.
    event_tx: broadcast::Sender<ChannelEvent>,

    /// The channel identifier string used in OpenCrust messages.
    channel_id: String,

    /// Guild IDs for slash command registration. Empty means global commands.
    guild_ids: Vec<u64>,

    /// Callback for processing incoming user messages.
    on_message: DiscordOnMessageFn,

    /// Group filter closure (decides whether to process group messages).
    group_filter: DiscordGroupFilter,
}

impl DiscordHandler {
    pub fn new(
        event_tx: broadcast::Sender<ChannelEvent>,
        channel_id: String,
        guild_ids: Vec<u64>,
        on_message: DiscordOnMessageFn,
        group_filter: DiscordGroupFilter,
    ) -> Self {
        Self {
            event_tx,
            channel_id,
            guild_ids,
            on_message,
            group_filter,
        }
    }

    fn emit(&self, event: ChannelEvent) {
        if let Err(e) = self.event_tx.send(event) {
            warn!("no subscribers for channel event: {e}");
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_message(
        &self,
        ctx: &Context,
        channel_id: serenity_model::ChannelId,
        user_id: String,
        user_name: String,
        text: String,
        is_group: bool,
        file: Option<DiscordFile>,
    ) {
        // Skip only if there is neither text nor an attached file.
        if text.trim().is_empty() && file.is_none() {
            return;
        }

        // Keep typing indicator alive while callback/streaming is in progress.
        let typing_http = ctx.http.clone();
        let typing_channel = channel_id;
        let typing_handle = tokio::spawn(async move {
            loop {
                let _ = typing_channel.broadcast_typing(&typing_http).await;
                tokio::time::sleep(Duration::from_secs(4)).await;
            }
        });

        let (delta_tx, mut delta_rx) = mpsc::channel::<String>(64);
        let on_message = Arc::clone(&self.on_message);
        let cb_channel_id = channel_id.to_string();
        let cb_user_id = user_id.clone();
        let cb_user_name = user_name.clone();
        let cb_text = text.clone();

        let callback_handle = tokio::spawn(async move {
            on_message(
                cb_channel_id,
                cb_user_id,
                cb_user_name,
                cb_text,
                is_group,
                file,
                Some(delta_tx),
            )
            .await
        });

        let mut accumulated = String::new();
        let mut sent: Vec<(MessageId, String)> = Vec::new();
        let mut first_delta_at: Option<Instant> = None;
        let mut last_update = Instant::now();

        while let Some(delta) = delta_rx.recv().await {
            accumulated.push_str(&delta);
            if first_delta_at.is_none() {
                first_delta_at = Some(Instant::now());
            }

            if first_delta_at
                .map(|t| t.elapsed() >= Duration::from_secs(1))
                .unwrap_or(false)
                && last_update.elapsed() >= Duration::from_millis(1000)
            {
                if let Err(e) =
                    sync_discord_chunks(ctx, channel_id, &accumulated, &mut sent, false).await
                {
                    warn!("failed to stream Discord update: {e}");
                    break;
                }
                last_update = Instant::now();
            }
        }

        typing_handle.abort();

        let result = callback_handle
            .await
            .unwrap_or_else(|e| Err(format!("task panic: {e}")));

        match result {
            Ok(ChannelResponse::Text(final_text)) => {
                if let Err(e) =
                    sync_discord_chunks(ctx, channel_id, &final_text, &mut sent, true).await
                {
                    warn!("failed to send Discord final response: {e}");
                }
            }
            Ok(ChannelResponse::Voice { text, audio }) => {
                // Send OGG/Opus audio as a file attachment.
                let attachment = serenity_model::CreateAttachment::bytes(audio, "voice.ogg");
                let msg = CreateMessage::new().add_file(attachment);
                if let Err(e) = channel_id.send_message(&ctx.http, msg).await {
                    warn!("failed to send Discord voice attachment: {e}");
                    // Fallback: send text
                    if let Err(e2) =
                        sync_discord_chunks(ctx, channel_id, &text, &mut sent, true).await
                    {
                        warn!("failed to send Discord voice fallback text: {e2}");
                    }
                }
            }
            Err(e) if e == "__blocked__" => {}
            Err(e) => {
                let err_text = format!("Sorry, an error occurred: {e}");
                if let Err(send_err) =
                    sync_discord_chunks(ctx, channel_id, &err_text, &mut sent, true).await
                {
                    warn!("failed to send Discord error response: {send_err}");
                }
            }
        }
    }

    async fn process_slash_command(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        slash: commands::DiscordSlashCommand,
    ) {
        if let Err(e) = command.defer(&ctx.http).await {
            warn!("failed to defer slash command response: {e}");
            return;
        }

        let user_id = command.user.id.to_string();
        let user_name = command
            .member
            .as_ref()
            .and_then(|m| m.nick.clone())
            .unwrap_or_else(|| command.user.name.clone());
        let text = format!("/{}", slash.as_str());

        let on_message = Arc::clone(&self.on_message);
        // Slash commands are exempt from group filtering - they're explicit interactions
        let result = on_message(
            command.channel_id.to_string(),
            user_id,
            user_name,
            text,
            false,
            None, // slash commands carry no file attachment
            None,
        )
        .await;

        let response_text = match result {
            Ok(r) => r.text().to_string(),
            Err(e) if e == "__blocked__" => "You are not authorized to use this bot.".to_string(),
            Err(e) => format!("Sorry, an error occurred: {e}"),
        };

        let response_text = convert::to_discord_markdown(&response_text);
        let chunks = convert::split_discord_chunks(&response_text);
        let first_chunk = chunks
            .first()
            .cloned()
            .unwrap_or_else(|| "\u{200B}".to_string());

        if let Err(e) = command
            .edit_response(
                &ctx.http,
                serenity_model::EditInteractionResponse::new().content(first_chunk),
            )
            .await
        {
            warn!("failed to edit deferred slash response: {e}");
            return;
        }

        if chunks.len() > 1 {
            for chunk in chunks.into_iter().skip(1) {
                if let Err(e) = command
                    .create_followup(
                        &ctx.http,
                        serenity_model::CreateInteractionResponseFollowup::new().content(chunk),
                    )
                    .await
                {
                    warn!("failed to create slash followup response: {e}");
                    break;
                }
            }
        }
    }
}

#[serenity::async_trait]
impl EventHandler for DiscordHandler {
    /// Fired when the bot successfully connects and is ready.
    async fn ready(&self, ctx: Context, ready: Ready) {
        info!(
            "Discord bot connected as {}#{} (guilds: {})",
            ready.user.name,
            ready
                .user
                .discriminator
                .map(|d| d.to_string())
                .unwrap_or_default(),
            ready.guilds.len()
        );

        let command_defs = commands::all_commands();
        if let Err(e) = commands::register_commands(&ctx, &self.guild_ids, &command_defs).await {
            warn!("failed to register discord slash commands: {e}");
        } else {
            info!("registered {} discord slash command(s)", command_defs.len());
        }

        self.emit(ChannelEvent::StatusChanged(ChannelStatus::Connected));
    }

    /// Fired when the bot resumes a previously interrupted gateway connection.
    async fn resume(&self, _ctx: Context, _: serenity_model::ResumedEvent) {
        info!("Discord gateway connection resumed");
        self.emit(ChannelEvent::StatusChanged(ChannelStatus::Connected));
    }

    /// Fired when a message is received in any channel the bot can see.
    async fn message(&self, ctx: Context, msg: SerenityMessage) {
        if msg.author.bot {
            return;
        }

        let is_group = msg.guild_id.is_some();
        let bot_id = ctx.cache.current_user().id;
        let is_mentioned = is_group && msg.mentions.iter().any(|u| u.id == bot_id);

        // Apply group filter before processing
        if is_group {
            if !(self.group_filter)(is_mentioned) {
                return;
            }
        }

        let opencrust_msg = convert::discord_message_to_opencrust(&msg, &self.channel_id);
        self.emit(ChannelEvent::MessageReceived(opencrust_msg));

        tracing::debug!(
            message_id = %msg.id,
            author = %msg.author.name,
            channel = %msg.channel_id,
            "received discord message"
        );

        // Download the first attachment (if any) before dispatching.
        let file = if let Some(attachment) = msg.attachments.first() {
            tracing::info!(
                filename = %attachment.filename,
                size = attachment.size,
                "discord: downloading attachment"
            );
            if attachment.size as usize > crate::MAX_DOWNLOAD_BYTES {
                warn!(
                    filename = %attachment.filename,
                    size = attachment.size,
                    limit = crate::MAX_DOWNLOAD_BYTES,
                    "discord: attachment too large, skipping download"
                );
                None
            } else {
                match attachment.download().await {
                    Ok(data) => Some(DiscordFile {
                        filename: attachment.filename.clone(),
                        data,
                        content_type: attachment.content_type.clone(),
                    }),
                    Err(e) => {
                        warn!("discord: failed to download attachment: {e}");
                        None
                    }
                }
            }
        } else {
            None
        };

        let text = if file.is_none()
            && is_mentioned
            && let Some(starter_text) =
                fetch_thread_starter_text_for_first_bot_turn(&ctx, &msg, bot_id).await
        {
            build_thread_starter_prompt(&msg.content, Some(&starter_text), bot_id)
        } else {
            msg.content.clone()
        };

        self.process_message(
            &ctx,
            msg.channel_id,
            msg.author.id.to_string(),
            msg.author
                .global_name
                .clone()
                .unwrap_or_else(|| msg.author.name.clone()),
            text,
            is_group,
            file,
        )
        .await;
    }

    /// Fired when a slash command interaction is created.
    async fn interaction_create(&self, ctx: Context, interaction: SerenityInteraction) {
        let SerenityInteraction::Command(command) = interaction else {
            return;
        };

        let Some(slash) = commands::DiscordSlashCommand::from_name(&command.data.name) else {
            return;
        };

        self.process_slash_command(&ctx, &command, slash).await;
    }

    /// Fired when a reaction is added to a message.
    async fn reaction_add(&self, _ctx: Context, reaction: serenity_model::Reaction) {
        let opencrust_msg = convert::reaction_to_opencrust(&reaction, &self.channel_id);

        tracing::debug!(
            emoji = ?reaction.emoji,
            message_id = %reaction.message_id,
            "received discord reaction"
        );

        self.emit(ChannelEvent::MessageReceived(opencrust_msg));
    }

    /// Fired when a thread is created.
    async fn thread_create(&self, _ctx: Context, thread: serenity_model::GuildChannel) {
        info!(
            thread_id = %thread.id,
            thread_name = %thread.name,
            "new discord thread created"
        );
    }
}

async fn sync_discord_chunks(
    ctx: &Context,
    channel_id: serenity_model::ChannelId,
    text: &str,
    sent: &mut Vec<(MessageId, String)>,
    is_final: bool,
) -> std::result::Result<(), String> {
    let formatted = convert::to_discord_markdown(text);
    let chunks = convert::split_discord_chunks(&formatted);

    for (idx, chunk) in chunks.iter().enumerate() {
        if idx < sent.len() {
            if sent[idx].1 != *chunk {
                channel_id
                    .edit_message(&ctx.http, sent[idx].0, EditMessage::new().content(chunk))
                    .await
                    .map_err(|e| format!("failed to edit Discord message: {e}"))?;
                sent[idx].1 = chunk.clone();
            }
        } else {
            let msg = channel_id
                .send_message(&ctx.http, CreateMessage::new().content(chunk))
                .await
                .map_err(|e| format!("failed to send Discord chunk: {e}"))?;
            sent.push((msg.id, chunk.clone()));
        }
    }

    if is_final && sent.len() > chunks.len() {
        for (id, _) in sent.drain(chunks.len()..) {
            let _ = channel_id.delete_message(&ctx.http, id).await;
        }
    }

    Ok(())
}

async fn fetch_thread_starter_text_for_first_bot_turn(
    ctx: &Context,
    msg: &SerenityMessage,
    bot_id: serenity_model::UserId,
) -> Option<String> {
    if msg.guild_id.is_none() {
        return None;
    }

    let thread = match msg.channel_id.to_channel(ctx).await {
        Ok(serenity_model::Channel::Guild(channel)) => channel,
        Ok(_) => return None,
        Err(e) => {
            debug!(
                channel_id = %msg.channel_id,
                "discord: failed to resolve channel before fetching thread starter: {e}"
            );
            return None;
        }
    };

    if thread.thread_metadata.is_none() {
        return None;
    }

    let Some(parent_id) = thread.parent_id else {
        return None;
    };

    match msg
        .channel_id
        .messages(ctx, serenity_model::GetMessages::new().limit(25))
        .await
    {
        Ok(messages) => {
            if messages
                .iter()
                .any(|m| m.id != msg.id && m.author.id == bot_id)
            {
                return None;
            }

            if let Some(starter_text) = messages
                .iter()
                .find(|m| m.kind == serenity_model::MessageType::ThreadStarterMessage)
                .and_then(discord_message_prompt_text)
            {
                return Some(starter_text);
            }
        }
        Err(e) => {
            warn!(
                thread_id = %msg.channel_id,
                "discord: failed to fetch thread history for starter message: {e}"
            );
        }
    }

    // For Discord threads created from a message, the thread channel id matches
    // the source message id in the parent channel.
    let source_message_id = serenity_model::MessageId::new(msg.channel_id.get());
    match parent_id.message(ctx, source_message_id).await {
        Ok(source_message) => discord_message_prompt_text(&source_message),
        Err(e) => {
            debug!(
                thread_id = %msg.channel_id,
                parent_channel_id = %parent_id,
                source_message_id = %source_message_id,
                "discord: failed to fetch parent thread starter message: {e}"
            );
            None
        }
    }
}

fn discord_message_prompt_text(message: &SerenityMessage) -> Option<String> {
    let text = message.content.trim();
    if !text.is_empty() {
        return Some(text.to_string());
    }

    message
        .referenced_message
        .as_deref()
        .and_then(discord_message_prompt_text)
}

fn build_thread_starter_prompt(
    current_text: &str,
    starter_text: Option<&str>,
    bot_id: serenity_model::UserId,
) -> String {
    let Some(starter_text) = starter_text.map(str::trim).filter(|text| !text.is_empty()) else {
        return current_text.to_string();
    };

    let current_without_bot_mention = strip_leading_bot_mentions(current_text, bot_id);
    if current_without_bot_mention.is_empty() {
        starter_text.to_string()
    } else {
        format!(
            "Thread starter message:\n{starter_text}\n\nCurrent message:\n{current_without_bot_mention}"
        )
    }
}

fn strip_leading_bot_mentions(text: &str, bot_id: serenity_model::UserId) -> &str {
    let mut remaining = text.trim_start();
    loop {
        let Some(stripped) = strip_one_leading_bot_mention(remaining, bot_id) else {
            break;
        };
        remaining = stripped.trim_start();
    }
    remaining.trim()
}

fn strip_one_leading_bot_mention(text: &str, bot_id: serenity_model::UserId) -> Option<&str> {
    let user_mention = format!("<@{}>", bot_id.get());
    let nickname_mention = format!("<@!{}>", bot_id.get());

    text.strip_prefix(&user_mention)
        .or_else(|| text.strip_prefix(&nickname_mention))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[test]
    fn handler_construction() {
        let (tx, _rx) = broadcast::channel::<ChannelEvent>(16);
        let on_msg: DiscordOnMessageFn =
            Arc::new(|_ch, _uid, _user, _text, _is_group, _file, _delta_tx| {
                Box::pin(async { Ok(ChannelResponse::Text("test".to_string())) })
            });
        let handler = DiscordHandler::new(
            tx,
            "discord".to_string(),
            vec![],
            on_msg,
            Arc::new(|_| true),
        );
        assert_eq!(handler.channel_id, "discord");
        assert!(handler.guild_ids.is_empty());
    }

    #[test]
    fn emit_with_no_subscribers_does_not_panic() {
        let (tx, _) = broadcast::channel::<ChannelEvent>(16);
        let on_msg: DiscordOnMessageFn =
            Arc::new(|_ch, _uid, _user, _text, _is_group, _file, _delta_tx| {
                Box::pin(async { Ok(ChannelResponse::Text("test".to_string())) })
            });
        let handler = DiscordHandler::new(
            tx,
            "discord".to_string(),
            vec![],
            on_msg,
            Arc::new(|_| true),
        );
        handler.emit(ChannelEvent::StatusChanged(ChannelStatus::Connected));
    }

    #[test]
    fn thread_starter_prompt_replaces_bot_mention_only() {
        let bot_id = serenity_model::UserId::new(42);

        let text = build_thread_starter_prompt("<@42>", Some("Check PRs"), bot_id);

        assert_eq!(text, "Check PRs");
    }

    #[test]
    fn thread_starter_prompt_replaces_nickname_bot_mention_only() {
        let bot_id = serenity_model::UserId::new(42);

        let text = build_thread_starter_prompt("<@!42>", Some("Check PRs"), bot_id);

        assert_eq!(text, "Check PRs");
    }

    #[test]
    fn thread_starter_prompt_includes_extra_current_text() {
        let bot_id = serenity_model::UserId::new(42);

        let text =
            build_thread_starter_prompt("<@42> use owner/repo tradee", Some("Check PRs"), bot_id);

        assert_eq!(
            text,
            "Thread starter message:\nCheck PRs\n\nCurrent message:\nuse owner/repo tradee"
        );
    }

    #[test]
    fn thread_starter_prompt_preserves_current_text_without_starter() {
        let bot_id = serenity_model::UserId::new(42);

        let text = build_thread_starter_prompt("<@42>", None, bot_id);

        assert_eq!(text, "<@42>");
    }

    #[test]
    fn thread_starter_prompt_does_not_strip_non_bot_mentions() {
        let bot_id = serenity_model::UserId::new(42);

        let text = build_thread_starter_prompt("<@7>", Some("Check PRs"), bot_id);

        assert_eq!(
            text,
            "Thread starter message:\nCheck PRs\n\nCurrent message:\n<@7>"
        );
    }
}
