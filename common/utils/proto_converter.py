# common/util/proto_converter.py
import json
from typing import Optional

# Importa as classes geradas pelo protoc
from common.generated import events_pb2
from common.schemas import events as pydantic_models

class ProtoConverter:
    """
    Ponte entre Pydantic (Aplicação) e Protobuf (Infraestrutura).
    Realiza a conversão bidirecional e trata tipos complexos (Dict -> JSON String).
    """

    # -------------------------------------------------------------------------
    # MESSAGE SENT (Core Message)
    # -------------------------------------------------------------------------
    @staticmethod
    def message_sent_to_proto(event: pydantic_models.MessageSentEvent) -> events_pb2.MessageSentEventProto:
        return events_pb2.MessageSentEventProto(
            message_id=event.message_id,
            conversation_id=event.conversation_id,
            sender_id=event.sender_id,
            timestamp=event.timestamp,
            message_type=event.message_type,
            content=event.content,
            # Tratamento de Dict: Serializa para JSON String
            attachments_json=json.dumps(event.attachments) if event.attachments else "",
            status=event.status,
            # Tratamento de Optional[int]: Protobuf usa 0 como default
            sequence_number=event.sequence_number if event.sequence_number is not None else 0,
            target_channels=event.target_channels
        )

    @staticmethod
    def message_sent_from_proto(proto: events_pb2.MessageSentEventProto) -> pydantic_models.MessageSentEvent:
        return pydantic_models.MessageSentEvent(
            message_id=proto.message_id,
            conversation_id=proto.conversation_id,
            sender_id=proto.sender_id,
            timestamp=proto.timestamp,
            message_type=proto.message_type,
            content=proto.content,
            # Tratamento de Dict: Deserializa de JSON String
            attachments=json.loads(proto.attachments_json) if proto.attachments_json else None,
            status=proto.status,
            # Tratamento de Optional[int]: 0 volta a ser None (ou mantém 0 se for válido na sua lógica)
            # Geralmente seq 0 não existe (começa em 1), então podemos tratar como None se necessário.
            sequence_number=proto.sequence_number if proto.sequence_number > 0 else None,
            target_channels=list(proto.target_channels)
        )

    # -------------------------------------------------------------------------
    # DELIVERY JOB (Worker Task)
    # -------------------------------------------------------------------------
    @staticmethod
    def delivery_job_to_proto(event: pydantic_models.DeliveryJobEvent) -> events_pb2.DeliveryJobEventProto:
        # Conversão recursiva do payload
        payload_proto = ProtoConverter.message_sent_to_proto(event.payload)
        
        return events_pb2.DeliveryJobEventProto(
            job_id=event.job_id,
            message_id=event.message_id,
            conversation_id=event.conversation_id,
            recipient_id=event.recipient_id,
            channel=event.channel,
            payload=payload_proto
        )

    @staticmethod
    def delivery_job_from_proto(proto: events_pb2.DeliveryJobEventProto) -> pydantic_models.DeliveryJobEvent:
        # Conversão recursiva reversa
        payload_pydantic = ProtoConverter.message_sent_from_proto(proto.payload)
        
        return pydantic_models.DeliveryJobEvent(
            job_id=proto.job_id,
            message_id=proto.message_id,
            conversation_id=proto.conversation_id,
            recipient_id=proto.recipient_id,
            channel=proto.channel,
            payload=payload_pydantic
        )

    # -------------------------------------------------------------------------
    # MESSAGE STATUS (Read Receipts)
    # -------------------------------------------------------------------------
    @staticmethod
    def message_status_to_proto(event: pydantic_models.MessageStatusEvent) -> events_pb2.MessageStatusEventProto:
        return events_pb2.MessageStatusEventProto(
            event_id=event.event_id,
            message_id=event.message_id,
            sequence_number=event.sequence_number,
            conversation_id=event.conversation_id,
            user_id=event.user_id,
            sender_id=event.sender_id,
            status=event.status,
            timestamp=event.timestamp
        )

    @staticmethod
    def message_status_from_proto(proto: events_pb2.MessageStatusEventProto) -> pydantic_models.MessageStatusEvent:
        return pydantic_models.MessageStatusEvent(
            event_id=proto.event_id,
            message_id=proto.message_id,
            sequence_number=proto.sequence_number,
            conversation_id=proto.conversation_id,
            user_id=proto.user_id,
            sender_id=proto.sender_id,
            status=proto.status,
            timestamp=proto.timestamp
        )

    # -------------------------------------------------------------------------
    # USER CREATED (Auth -> Metadata Sync)
    # -------------------------------------------------------------------------
    @staticmethod
    def user_created_to_proto(event: pydantic_models.UserCreatedEvent) -> events_pb2.UserCreatedEventProto:
        return events_pb2.UserCreatedEventProto(
            event_id=event.event_id,
            user_id=event.user_id,
            username=event.username,
            email=event.email,
            name=event.name,
            timestamp=event.timestamp
        )

    @staticmethod
    def user_created_from_proto(proto: events_pb2.UserCreatedEventProto) -> pydantic_models.UserCreatedEvent:
        return pydantic_models.UserCreatedEvent(
            event_id=proto.event_id,
            user_id=proto.user_id,
            username=proto.username,
            email=proto.email,
            name=proto.name,
            timestamp=proto.timestamp
        )

    # -------------------------------------------------------------------------
    # PUSH NOTIFICATION (Mobile Alert)
    # -------------------------------------------------------------------------
    @staticmethod
    def push_notification_to_proto(event: pydantic_models.PushNotificationEvent) -> events_pb2.PushNotificationEventProto:
        return events_pb2.PushNotificationEventProto(
            notification_id=event.notification_id,
            user_id=event.user_id,
            title=event.title,
            body=event.body,
            # Dict -> JSON String
            data_json=json.dumps(event.data) if event.data else "",
            timestamp=event.timestamp
        )

    @staticmethod
    def push_notification_from_proto(proto: events_pb2.PushNotificationEventProto) -> pydantic_models.PushNotificationEvent:
        return pydantic_models.PushNotificationEvent(
            notification_id=proto.notification_id,
            user_id=proto.user_id,
            title=proto.title,
            body=proto.body,
            # JSON String -> Dict
            data=json.loads(proto.data_json) if proto.data_json else None,
            timestamp=proto.timestamp
        )