"""Configuration defintion
"""
import argparse
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class KsnapConfig:

    action: str
    brokers: List[str]
    topics: List[str]
    data: str
    threads: int
    ignore_missing_topics: bool
    consumer_timeout: int

    @classmethod
    def from_cli_args(cls, cli_args: Optional[List[str]] = None):
        parser = KsnapConfig._get_parser()
        args = parser.parse_args(cli_args) if cli_args else parser.parse_args()
        return cls(
            action=args.action,
            brokers=args.brokers.split(','),
            topics=args.topics.split(','),
            data=args.data,
            threads=args.threads,
            ignore_missing_topics=args.ignore_missing_topics,
            consumer_timeout=args.consumer_timeout
        )

    @staticmethod
    def _get_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'action',  choices=['backup', 'restore']
        )
        parser.add_argument(
            '-b',
            '--brokers',
            help='Comma-separated list of brokers in format `host:port`.',
            default='localhost:9092',
            required=True,
        )
        parser.add_argument(
            '-t',
            '--topics',
            help='Comma-separated list of topics',
            required=True,
        )
        parser.add_argument(
            '-d',
            '--data',
            help='Directory where this tool will store data or read from',
            required=True,
        )
        parser.add_argument(
            '--threads',
            help='No of threads using for writing messages to Kafka',
            default=8,
            type=int
        )
        parser.add_argument(
            '--ignore-missing-topics',
            help='Ignore missing topics in Kafka broker',
            action='store_true'
        )
        parser.add_argument(
            '--consumer-timeout',
            help='Timeout in seconds for consuming topics',
            default=300,
            type=int
        )
        return parser
