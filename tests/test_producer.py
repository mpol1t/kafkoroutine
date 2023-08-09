from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from kafkoroutine.producer import AsyncKafkaProducer


def test_init():
    with patch('asyncio.get_event_loop', return_value=MagicMock()) as mock_get_event_loop:
        executor = ThreadPoolExecutor()
        loop = MagicMock()
        bootstrap_servers = ["localhost:9092"]
        producer_kwargs = {"value_serializer": lambda v: str(v).encode('utf-8')}

        producer = AsyncKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            executor=executor,
            loop=loop,
            **producer_kwargs,
        )

        assert producer.loop is loop
        assert producer.run_in_executor.func is loop.run_in_executor
        assert producer.bootstrap_servers == bootstrap_servers
        assert producer.producer is None
        assert producer.producer_kwargs == producer_kwargs
        mock_get_event_loop.assert_not_called()

        # Test if executor is correctly used by run_in_executor
        mock_function = MagicMock()
        producer.run_in_executor(mock_function)
        loop.run_in_executor.assert_called_once_with(executor, mock_function)


def test_init_no_loop():
    with patch('asyncio.get_event_loop', return_value=MagicMock()) as mock_get_event_loop:
        executor = ThreadPoolExecutor()
        bootstrap_servers = ["localhost:9092"]
        producer_kwargs = {"value_serializer": lambda v: str(v).encode('utf-8')}

        producer = AsyncKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            executor=executor,
            **producer_kwargs,
        )

        assert producer.loop is mock_get_event_loop.return_value
        assert producer.run_in_executor.func is mock_get_event_loop.return_value.run_in_executor
        assert producer.bootstrap_servers == bootstrap_servers
        assert producer.producer is None
        assert producer.producer_kwargs == producer_kwargs
        mock_get_event_loop.assert_called_once()

        # Test if executor is correctly used by run_in_executor
        mock_function = MagicMock()
        producer.run_in_executor(mock_function)
        mock_get_event_loop.return_value.run_in_executor.assert_called_once_with(executor, mock_function)


@pytest.mark.asyncio
async def test_aenter():
    with patch('asyncio.get_event_loop', return_value=MagicMock()):
        executor = ThreadPoolExecutor()
        loop = MagicMock()
        bootstrap_servers = ["localhost:9092"]
        producer_kwargs = {"value_serializer": lambda v: str(v).encode('utf-8')}

        producer = AsyncKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            executor=executor,
            loop=loop,
            **producer_kwargs,
        )

        producer.create_producer = AsyncMock()

        # At first, producer.producer is None so create_producer should be called
        await producer.__aenter__()
        producer.create_producer.assert_called_once()

        # Now, producer.producer should not be None, so create_producer should not be called again
        producer.producer = MagicMock()
        await producer.__aenter__()
        producer.create_producer.assert_called_once()  # still only called once


@pytest.mark.asyncio
async def test_aexit():
    with patch('asyncio.get_event_loop', return_value=MagicMock()):
        executor = ThreadPoolExecutor()
        loop = MagicMock()
        bootstrap_servers = ["localhost:9092"]
        producer_kwargs = {"value_serializer": lambda v: str(v).encode('utf-8')}

        producer = AsyncKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            executor=executor,
            loop=loop,
            **producer_kwargs,
        )

        producer.producer = MagicMock()
        producer.producer.close = AsyncMock()
        producer.run_in_executor = AsyncMock(side_effect=lambda f: f())

        # Call __aexit__ method
        await producer.__aexit__(None, None, None)

        # Check that close was called
        producer.producer.close.assert_called_once()


@pytest.mark.asyncio
async def test_create_producer():
    with patch('kafkoroutine.producer.KafkaProducer') as mock_kafka_producer:
        executor = ThreadPoolExecutor()
        bootstrap_servers = ["localhost:9092"]
        producer_kwargs = {"value_serializer": lambda v: str(v).encode('utf-8')}

        producer = AsyncKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            executor=executor,
            **producer_kwargs,
        )

        # Get the event loop from the AsyncKafkaProducer instance.
        loop = producer.loop

        mock_kafka_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_kafka_producer_instance

        # Use the event loop from the AsyncKafkaProducer instance to create the Future.
        future = loop.create_future()
        future.set_result(mock_kafka_producer_instance)

        # Define the side effect function.
        def side_effect_run_in_executor(func):
            func()
            return future

        with patch.object(producer, 'run_in_executor', side_effect=side_effect_run_in_executor) as mock_run_in_executor:
            await producer.create_producer()
            mock_run_in_executor.assert_called_once()
            assert producer.producer == mock_kafka_producer_instance
            mock_kafka_producer.assert_called_once_with(bootstrap_servers=bootstrap_servers, **producer_kwargs)


@pytest.mark.asyncio
async def test_send():
    with patch('kafkoroutine.producer.KafkaProducer') as mock_kafka_producer:
        # Set up test variables
        bootstrap_servers = ["localhost:9092"]
        producer_kwargs = {"value_serializer": lambda v: str(v).encode('utf-8')}
        topic = "sample_topic"
        value = "sample_message"

        # Initialize the mocked producer
        mock_kafka_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_kafka_producer_instance
        mock_send = MagicMock(return_value="message_sent")
        mock_kafka_producer_instance.send = mock_send

        # Initialize AsyncKafkaProducer instance
        producer = AsyncKafkaProducer(bootstrap_servers=bootstrap_servers, executor=ThreadPoolExecutor(),
                                      **producer_kwargs)

        # Create the producer and send a message
        await producer.create_producer()
        result = await producer.send(topic, value)

        # Assertions
        mock_send.assert_called_once_with(topic, value)
        assert result == "message_sent"
