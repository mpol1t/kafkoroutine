from __future__ import annotations

import asyncio
import logging
from asyncio import AbstractEventLoop
from concurrent.futures import Executor
from functools import partial
from typing import List, Optional, Any

from kafka import KafkaConsumer


class AsyncKafkaConsumer:
    """
    An asynchronous Kafka Consumer wrapper.

    This class is an asyncio-compatible version of the KafkaConsumer
    from the kafka-python library.
    """

    def __init__(
            self,
            topics: List[str] | str,
            bootstrap_servers: List[str] | str,
            executor: Optional[Executor] = None,
            loop: Optional[AbstractEventLoop] = None,
            **kwargs: Any
    ) -> None:
        """
        Initialize the AsyncKafkaConsumer.

        :param topics: The Kafka topic to consume from.
        :param bootstrap_servers: The Kafka server to connect to.
        :param executor: Executor to run blocking operations in.
        :param loop: Optional asyncio event loop.
        :param kwargs: Additional arguments to pass to the KafkaConsumer.
        """
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        self.run_in_executor = partial(self.loop.run_in_executor, executor)
        self.bootstrap_servers = bootstrap_servers

        self.topics = topics
        self.consumer = None
        self.iterator = None

        self.consumer_kwargs = kwargs

    async def __aenter__(self) -> AsyncKafkaConsumer:
        """
        Enter the async context manager, creating the consumer if needed.
        """
        if self.consumer is None:
            await self.create_consumer()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """
        Exit the async context manager, closing the consumer.
        """
        await self.run_in_executor(self.consumer.close)

    async def create_consumer(self) -> None:
        """
        Create the KafkaConsumer and its iterator in the executor.
        """

        def create_consumer() -> KafkaConsumer:
            return KafkaConsumer(self.topics, bootstrap_servers=self.bootstrap_servers, **self.consumer_kwargs)

        self.consumer = await self.run_in_executor(create_consumer)
        self.iterator = iter(self.consumer)

    def __aiter__(self) -> AsyncKafkaConsumer:
        """
        Return self as the async iterator.
        """
        return self

    async def __anext__(self) -> None:
        """
        Get the next message from the KafkaConsumer.

        If no more messages are available, raise StopAsyncIteration to end the iteration. If an exception occurs while
        getting the next message, log the exception and raise it again.
        """
        while True:
            try:
                return await self.run_in_executor(next, self.iterator)
            except StopIteration:
                raise StopAsyncIteration
            except Exception as e:
                logging.exception("Exception occurred while consuming messages", exc_info=e)
                raise
