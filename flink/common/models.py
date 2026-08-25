from pydantic import BaseModel


class KafkaSourceParams(BaseModel):
    topic: str
    group_id: str
    bootstrap_servers: str


class KafkaSinkParams(BaseModel):
    topic: str
    bootstrap_servers: str
