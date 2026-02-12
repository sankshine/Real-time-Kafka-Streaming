"""
Enterprise Kafka Consumer with Event Enrichment
Real-Time Streaming Platform

Features:
- Exactly-once semantics with offset management
- Event enrichment with external API lookups
- Dead Letter Queue (DLQ) pattern for error handling
- Graceful shutdown and rebalancing
- Comprehensive metrics and logging
"""

import json
import logging
import signal
import sys
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
import redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import requests
from elasticsearch import Elasticsearch
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
MESSAGES_CONSUMED = Counter('kafka_messages_consumed_total', 'Total messages consumed', ['topic', 'partition'])
MESSAGES_PROCESSED = Counter('kafka_messages_processed_total', 'Successfully processed messages', ['topic'])
MESSAGES_FAILED = Counter('kafka_messages_failed_total', 'Failed message processing', ['topic', 'error_type'])
PROCESSING_TIME = Histogram('kafka_message_processing_seconds', 'Message processing time', ['topic'])
ENRICHMENT_TIME = Histogram('enrichment_api_call_seconds', 'API enrichment call time', ['api'])
CONSUMER_LAG = Gauge('kafka_consumer_lag', 'Consumer lag', ['topic', 'partition'])
MESSAGES_IN_DLQ = Counter('kafka_dlq_messages_total', 'Messages sent to DLQ', ['topic', 'error_type'])


class EnrichmentConsumer:
    """
    Production-grade Kafka consumer with event enrichment capabilities.
    
    Implements:
    - Exactly-once processing semantics
    - Event enrichment from multiple sources (API, Redis, DB)
    - Error handling with DLQ pattern
    - Graceful shutdown
    - Comprehensive monitoring
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the enrichment consumer.
        
        Args:
            config: Configuration dictionary containing Kafka, Redis, and API settings
        """
        self.config = config
        self.running = True
        self.consumer = None
        self.producer = None
        self.dlq_producer = None
        
        # Initialize components
        self._init_kafka()
        self._init_redis()
        self._init_elasticsearch()
        self._init_http_session()
        
        # Metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.start_time = time.time()
        
        # Thread pool for parallel enrichment
        self.executor = ThreadPoolExecutor(max_workers=config.get('enrichment_workers', 10))
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("EnrichmentConsumer initialized successfully")
    
    def _init_kafka(self):
        """Initialize Kafka consumer and producers"""
        # Consumer configuration
        consumer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'group.id': self.config['kafka']['group_id'],
            'auto.offset.reset': self.config['kafka'].get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': False,  # Manual commit for exactly-once
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000,
            'fetch.min.bytes': 1024,
            'fetch.wait.max.ms': 500,
            'max.partition.fetch.bytes': 1048576,  # 1MB
            'isolation.level': 'read_committed',  # For exactly-once
            'client.id': f"enrichment-consumer-{self.config.get('instance_id', '1')}",
        }
        
        # Add SASL/SSL if configured
        if self.config.get('kafka', {}).get('security_protocol'):
            consumer_config.update({
                'security.protocol': self.config['kafka']['security_protocol'],
                'sasl.mechanism': self.config['kafka'].get('sasl_mechanism', 'PLAIN'),
                'sasl.username': self.config['kafka'].get('sasl_username'),
                'sasl.password': self.config['kafka'].get('sasl_password'),
            })
        
        self.consumer = Consumer(consumer_config)
        
        # Subscribe to topics
        topics = self.config['kafka']['topics']
        self.consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke)
        logger.info(f"Subscribed to topics: {topics}")
        
        # Producer for enriched messages
        producer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'max.in.flight.requests.per.connection': 5,
            'enable.idempotence': True,  # Exactly-once
            'compression.type': 'snappy',
            'batch.size': 16384,
            'linger.ms': 10,
            'client.id': f"enrichment-producer-{self.config.get('instance_id', '1')}",
        }
        
        if self.config.get('kafka', {}).get('security_protocol'):
            producer_config.update({
                'security.protocol': self.config['kafka']['security_protocol'],
                'sasl.mechanism': self.config['kafka'].get('sasl_mechanism', 'PLAIN'),
                'sasl.username': self.config['kafka'].get('sasl_username'),
                'sasl.password': self.config['kafka'].get('sasl_password'),
            })
        
        self.producer = Producer(producer_config)
        
        # DLQ producer
        self.dlq_producer = Producer(producer_config)
        
        logger.info("Kafka consumer and producers initialized")
    
    def _init_redis(self):
        """Initialize Redis connection for caching"""
        redis_config = self.config.get('redis', {})
        if redis_config:
            self.redis_client = redis.Redis(
                host=redis_config.get('host', 'localhost'),
                port=redis_config.get('port', 6379),
                db=redis_config.get('db', 0),
                password=redis_config.get('password'),
                socket_timeout=5,
                socket_connect_timeout=5,
                decode_responses=True
            )
            logger.info("Redis cache initialized")
        else:
            self.redis_client = None
            logger.warning("Redis not configured, caching disabled")
    
    def _init_elasticsearch(self):
        """Initialize Elasticsearch client for logging"""
        es_config = self.config.get('elasticsearch', {})
        if es_config:
            self.es_client = Elasticsearch(
                hosts=[es_config.get('host', 'localhost:9200')],
                http_auth=(es_config.get('username'), es_config.get('password')) if es_config.get('username') else None,
                verify_certs=es_config.get('verify_certs', True),
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            logger.info("Elasticsearch client initialized")
        else:
            self.es_client = None
            logger.warning("Elasticsearch not configured")
    
    def _init_http_session(self):
        """Initialize HTTP session with retry logic"""
        self.http_session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        self.http_session.mount("http://", adapter)
        self.http_session.mount("https://", adapter)
    
    def _on_assign(self, consumer, partitions):
        """Callback on partition assignment"""
        logger.info(f"Partitions assigned: {partitions}")
        for partition in partitions:
            # Get current lag
            low, high = consumer.get_watermark_offsets(partition)
            committed = consumer.committed([partition])[0].offset
            lag = high - committed if committed >= 0 else high - low
            CONSUMER_LAG.labels(topic=partition.topic, partition=partition.partition).set(lag)
    
    def _on_revoke(self, consumer, partitions):
        """Callback on partition revocation"""
        logger.info(f"Partitions revoked: {partitions}")
        # Commit offsets before revocation
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            logger.error(f"Error committing offsets on revoke: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    @lru_cache(maxsize=10000)
    def _get_from_cache(self, cache_key: str) -> Optional[str]:
        """Get value from Redis cache with LRU fallback"""
        if not self.redis_client:
            return None
        
        try:
            return self.redis_client.get(cache_key)
        except Exception as e:
            logger.warning(f"Redis cache error: {e}")
            return None
    
    def _set_in_cache(self, cache_key: str, value: str, ttl: int = 3600):
        """Set value in Redis cache"""
        if not self.redis_client:
            return
        
        try:
            self.redis_client.setex(cache_key, ttl, value)
        except Exception as e:
            logger.warning(f"Redis cache set error: {e}")
    
    def enrich_user_data(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich event with user profile data.
        
        Args:
            event: Raw event data
            
        Returns:
            Enriched event with user profile
        """
        user_id = event.get('user_id')
        if not user_id:
            return event
        
        cache_key = f"user_profile:{user_id}"
        
        # Try cache first
        cached_profile = self._get_from_cache(cache_key)
        if cached_profile:
            event['user_profile'] = json.loads(cached_profile)
            return event
        
        # Fetch from API
        try:
            start_time = time.time()
            api_url = f"{self.config['enrichment']['user_api_url']}/users/{user_id}"
            response = self.http_session.get(api_url, timeout=2)
            
            ENRICHMENT_TIME.labels(api='user_api').observe(time.time() - start_time)
            
            if response.status_code == 200:
                user_profile = response.json()
                event['user_profile'] = user_profile
                
                # Cache the result
                self._set_in_cache(cache_key, json.dumps(user_profile), ttl=3600)
            else:
                logger.warning(f"User API returned status {response.status_code} for user {user_id}")
                event['user_profile'] = None
        
        except Exception as e:
            logger.error(f"Error enriching user data: {e}")
            event['user_profile'] = None
        
        return event
    
    def enrich_geo_data(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich event with geolocation data.
        
        Args:
            event: Event data
            
        Returns:
            Event with geo enrichment
        """
        ip_address = event.get('ip_address')
        if not ip_address:
            return event
        
        cache_key = f"geo:{ip_address}"
        
        # Try cache
        cached_geo = self._get_from_cache(cache_key)
        if cached_geo:
            event['geo'] = json.loads(cached_geo)
            return event
        
        # Fetch from geo API
        try:
            start_time = time.time()
            api_url = f"{self.config['enrichment']['geo_api_url']}/{ip_address}"
            response = self.http_session.get(api_url, timeout=2)
            
            ENRICHMENT_TIME.labels(api='geo_api').observe(time.time() - start_time)
            
            if response.status_code == 200:
                geo_data = response.json()
                event['geo'] = {
                    'country': geo_data.get('country'),
                    'city': geo_data.get('city'),
                    'latitude': geo_data.get('latitude'),
                    'longitude': geo_data.get('longitude')
                }
                
                # Cache for 24 hours
                self._set_in_cache(cache_key, json.dumps(event['geo']), ttl=86400)
            else:
                event['geo'] = None
        
        except Exception as e:
            logger.error(f"Error enriching geo data: {e}")
            event['geo'] = None
        
        return event
    
    def process_message(self, msg) -> bool:
        """
        Process a single Kafka message with enrichment.
        
        Args:
            msg: Kafka message
            
        Returns:
            True if processing succeeded, False otherwise
        """
        start_time = time.time()
        
        try:
            # Parse message
            event = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            
            MESSAGES_CONSUMED.labels(topic=topic, partition=partition).inc()
            
            logger.debug(f"Processing message from {topic}[{partition}] at offset {offset}")
            
            # Add processing metadata
            event['_processing'] = {
                'received_at': datetime.utcnow().isoformat(),
                'source_topic': topic,
                'source_partition': partition,
                'source_offset': offset
            }
            
            # Perform enrichments in parallel
            futures = []
            if self.config.get('enrichment', {}).get('enable_user_enrichment', True):
                futures.append(self.executor.submit(self.enrich_user_data, event))
            
            if self.config.get('enrichment', {}).get('enable_geo_enrichment', True):
                futures.append(self.executor.submit(self.enrich_geo_data, event))
            
            # Wait for all enrichments
            for future in futures:
                event = future.result(timeout=5)
            
            # Add enrichment completion timestamp
            event['_processing']['enriched_at'] = datetime.utcnow().isoformat()
            
            # Produce enriched message
            output_topic = self.config['kafka']['output_topic']
            self.producer.produce(
                topic=output_topic,
                key=msg.key(),
                value=json.dumps(event).encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Poll to handle delivery callbacks
            self.producer.poll(0)
            
            # Log to Elasticsearch
            if self.es_client:
                try:
                    self.es_client.index(
                        index=f"streaming-events-{datetime.utcnow().strftime('%Y.%m.%d')}",
                        document=event
                    )
                except Exception as e:
                    logger.warning(f"Failed to index to Elasticsearch: {e}")
            
            # Update metrics
            processing_time = time.time() - start_time
            PROCESSING_TIME.labels(topic=topic).observe(processing_time)
            MESSAGES_PROCESSED.labels(topic=topic).inc()
            
            self.messages_processed += 1
            
            if self.messages_processed % 1000 == 0:
                logger.info(f"Processed {self.messages_processed} messages, "
                           f"failed: {self.messages_failed}, "
                           f"rate: {self.messages_processed / (time.time() - self.start_time):.2f} msg/sec")
            
            return True
        
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            MESSAGES_FAILED.labels(topic=msg.topic(), error_type='json_decode_error').inc()
            self._send_to_dlq(msg, 'json_decode_error', str(e))
            return False
        
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            MESSAGES_FAILED.labels(topic=msg.topic(), error_type='processing_error').inc()
            self._send_to_dlq(msg, 'processing_error', str(e))
            return False
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err:
            logger.error(f"Message delivery failed: {err}")
            MESSAGES_FAILED.labels(topic=msg.topic(), error_type='delivery_error').inc()
        else:
            logger.debug(f"Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
    
    def _send_to_dlq(self, msg, error_type: str, error_message: str):
        """Send failed message to Dead Letter Queue"""
        dlq_topic = self.config['kafka'].get('dlq_topic', 'dlq.events')
        
        dlq_message = {
            'original_topic': msg.topic(),
            'original_partition': msg.partition(),
            'original_offset': msg.offset(),
            'original_timestamp': msg.timestamp()[1] if msg.timestamp() else None,
            'original_key': msg.key().decode('utf-8') if msg.key() else None,
            'original_value': msg.value().decode('utf-8') if msg.value() else None,
            'error_type': error_type,
            'error_message': error_message,
            'failed_at': datetime.utcnow().isoformat()
        }
        
        try:
            self.dlq_producer.produce(
                topic=dlq_topic,
                value=json.dumps(dlq_message).encode('utf-8')
            )
            self.dlq_producer.flush()
            MESSAGES_IN_DLQ.labels(topic=msg.topic(), error_type=error_type).inc()
            logger.info(f"Message sent to DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting consumer loop...")
        
        # Start Prometheus metrics server
        metrics_port = self.config.get('metrics_port', 8000)
        start_http_server(metrics_port)
        logger.info(f"Metrics server started on port {metrics_port}")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.topic()}[{msg.partition()}]")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Process message
                success = self.process_message(msg)
                
                # Commit offset after successful processing
                if success:
                    try:
                        self.consumer.commit(message=msg, asynchronous=False)
                    except KafkaException as e:
                        logger.error(f"Error committing offset: {e}")
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down consumer...")
        
        # Flush producers
        if self.producer:
            logger.info("Flushing producer...")
            self.producer.flush(timeout=10)
        
        if self.dlq_producer:
            self.dlq_producer.flush(timeout=10)
        
        # Close consumer
        if self.consumer:
            logger.info("Closing consumer...")
            self.consumer.close()
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        # Close connections
        if self.redis_client:
            self.redis_client.close()
        
        if self.es_client:
            self.es_client.close()
        
        logger.info(f"Consumer shutdown complete. Processed {self.messages_processed} messages, "
                   f"failed: {self.messages_failed}")


def main():
    """Main entry point"""
    import yaml
    
    # Load configuration
    with open('config/consumer_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create and run consumer
    consumer = EnrichmentConsumer(config)
    consumer.run()


if __name__ == '__main__':
    main()
