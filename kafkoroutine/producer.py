from __future__ import annotations

import asyncio
from asyncio import AbstractEventLoop
from concurrent.futures import Executor
from functools import partial
from typing import Any, List, Optional

from kafka import KafkaProducer


class AsyncKafkaProducer:
    """
    An asynchronous Kafka Producer wrapper.

    This class provides an asyncio-compatible version of the KafkaProducer
    from the kafka-python library.
    """

    def __init__(
            self,
            bootstrap_servers: List[str] | str,
            executor: Optional[Executor] = None,
            loop: Optional[AbstractEventLoop] = None,
            **kwargs: Any
    ) -> None:
        """
        Initialize the AsyncKafkaProducer.

        :param bootstrap_servers: The Kafka server to connect to.
        :param executor: Executor to run blocking operations in.
        :param loop: Optional asyncio event loop.
        :param kwargs: Additional arguments to pass to the KafkaProducer.
        """
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        self.run_in_executor = partial(self.loop.run_in_executor, executor)
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

        self.producer_kwargs = kwargs

    async def __aenter__(self) -> AsyncKafkaProducer:
        """
        Enter the async context manager, creating the producer if needed.
        """
        if self.producer is None:
            await self.create_producer()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """
        Exit the async context manager, closing the producer.
        """
        await self.run_in_executor(self.producer.close)

    async def create_producer(self) -> None:
        """
        Create the KafkaProducer in the executor.
        """

        def create_producer():
            return KafkaProducer(bootstrap_servers=self.bootstrap_servers, **self.producer_kwargs)

        self.producer = await self.run_in_executor(create_producer)

    async def send(self, topic: str, value: Any) -> Any:
        """
        Send a message to the specified Kafka topic.

        The actual sending is performed in the executor to not block the asyncio event loop.
        :param topic: The Kafka topic to send the message to.
        :param value: The message to send.
        :return: The result of the send operation.
        """
        return await self.run_in_executor(self.producer.send, topic, value)
