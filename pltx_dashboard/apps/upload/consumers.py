import json
from channels.generic.websocket import AsyncWebsocketConsumer


class UploadProgressConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.user_id = self.scope["session"].get("user_id")

        # Accept first so channels doesn't log a noisy 403 for every attempt.
        # Then immediately close with a custom code (4403 = not authenticated)
        # so the frontend can distinguish "auth required" from a network error
        # and stop retrying endlessly.
        await self.accept()

        if not self.user_id:
            await self.close(code=4403)
            return

        self.group_name = f"user_{self.user_id}"
        await self.channel_layer.group_add(self.group_name, self.channel_name)

    async def disconnect(self, _close_code):
        if hasattr(self, "group_name"):
            await self.channel_layer.group_discard(self.group_name, self.channel_name)

    async def upload_progress(self, event):
        await self.send(
            text_data=json.dumps(
                {"message": event["message"], "status": event["status"]}
            )
        )
