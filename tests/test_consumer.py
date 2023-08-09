import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, MagicMock, AsyncMock

import pytest
from kafka.errors import NoBrokersAvailable, KafkaError

from async_kafka.consumer import AsyncKafkaConsumer


@pytest.mark.asyncio
async def test_initialization():
    # Given
    topics = ["test_topic"]
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    # When
    consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor)

    # Then
    assert consumer.topics == topics
    assert consumer.bootstrap_servers == bootstrap_servers
    assert consumer.loop == asyncio.get_event_loop()
    assert consumer.consumer is None
    assert consumer.iterator is None


@pytest.mark.asyncio
async def test_default_loop_initialization():
    # Given
    topics = ["test_topic"]
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    # When
    consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor)

    # Then
    assert consumer.loop is asyncio.get_event_loop()


# If you also want to check that providing a loop works correctly
@pytest.mark.asyncio
async def test_custom_loop_initialization():
    # Given
    topics = ["test_topic"]
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()
    custom_loop = asyncio.new_event_loop()

    # When
    consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor, loop=custom_loop)

    # Then
    assert consumer.loop is custom_loop


@pytest.mark.asyncio
async def test_create_consumer_successful_creation():
    # Given
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()
    consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor)

    def side_effect_kafka_consumer(*args, **kwargs):
        return MagicMock()

    # We're mocking kafka.KafkaConsumer so that we don't hit a real Kafka instance.
    with patch('async_kafka.consumer.KafkaConsumer', side_effect=side_effect_kafka_consumer):
        # When
        await consumer.create_consumer()

        # Then
        assert consumer.consumer is not None
        assert consumer.iterator is not None


@pytest.mark.asyncio
async def test_create_consumer_with_invalid_configuration():
    topics = "test_topic"
    bootstrap_servers = ["invalid_server:9092"]
    executor = ThreadPoolExecutor()
    consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor)

    with pytest.raises(NoBrokersAvailable):
        await consumer.create_consumer()


@pytest.mark.asyncio
async def test_enter_async_context_manager():
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    # Try being more specific about the patch, depending on the actual import in AsyncKafkaConsumer.
    with patch('async_kafka.consumer.KafkaConsumer', return_value=MagicMock()) as mock_kafka_consumer:
        async with AsyncKafkaConsumer(topics, bootstrap_servers, executor) as consumer:
            assert consumer.consumer == mock_kafka_consumer.return_value


@pytest.mark.asyncio
async def test_exit_async_context_manager():
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    mock_kafka_consumer_instance = MagicMock()

    # Mock the KafkaConsumer
    with patch('async_kafka.consumer.KafkaConsumer', return_value=mock_kafka_consumer_instance):
        consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor)

        # Manually enter and exit the async context to ensure __aexit__ gets called.
        await consumer.__aenter__()
        await consumer.__aexit__(None, None, None)

        # Check if the close method has been called once after exiting the async context
        mock_kafka_consumer_instance.close.assert_called_once()


@pytest.mark.asyncio
async def test_error_within_async_context_manager():
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    mock_kafka_consumer_instance = MagicMock()

    with patch('kafka.KafkaConsumer', return_value=mock_kafka_consumer_instance):
        with pytest.raises(KafkaError, match="Simulated Kafka Error"):
            async with AsyncKafkaConsumer(topics, bootstrap_servers, executor) as consumer:
                raise KafkaError("Simulated Kafka Error")


@pytest.mark.asyncio
async def test_error_within_async_context_manager():
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    # Here, we're setting the side_effect on the class instantiation itself,
    # not on an instance of the class.
    with patch('async_kafka.consumer.KafkaConsumer', side_effect=KafkaError("Simulated Kafka Error")):
        with pytest.raises(KafkaError, match="Simulated Kafka Error"):
            async with AsyncKafkaConsumer(topics, bootstrap_servers, executor) as consumer:
                pass


@pytest.mark.asyncio
async def test_aiter_method():
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    # Create an instance of AsyncKafkaConsumer
    consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor)

    # Ensure that the async iterator of the consumer is the consumer itself
    assert consumer.__aiter__() is consumer


@pytest.mark.asyncio
async def test_normal_consumption():
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    mock_message = MagicMock()

    with patch.object(AsyncKafkaConsumer, "__anext__", new_callable=AsyncMock, return_value=mock_message):
        async with AsyncKafkaConsumer(topics, bootstrap_servers, executor) as consumer:
            message = await consumer.__anext__()
            assert message == mock_message


@pytest.mark.asyncio
async def test_general_exception_during_consuming():
    topics = "test_topic"
    bootstrap_servers = ["localhost:9092"]
    executor = ThreadPoolExecutor()

    custom_exception = Exception("Custom error during consumption")

    mock_kafka_consumer_instance = MagicMock()

    with patch('async_kafka.consumer.KafkaConsumer', return_value=mock_kafka_consumer_instance):
        consumer = AsyncKafkaConsumer(topics, bootstrap_servers, executor)

        # Mocking run_in_executor to raise the exception
        consumer.run_in_executor = MagicMock(side_effect=custom_exception)

        with patch.object(logging, 'exception') as mock_log:
            with pytest.raises(Exception, match="Custom error during consumption"):
                await consumer.__anext__()

            mock_log.assert_called_once()
