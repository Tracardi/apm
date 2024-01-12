import logging
import os
from asyncio import sleep

from aio.loop import MainLoop
from config import config
from tracardi.config import tracardi
from tracardi.context import ServerContext, Context
from tracardi.domain.profile import Profile
from tracardi.service.elastic.connection import wait_for_connection
from tracardi.service.profile_deduplicator import deduplicate_profile
from tracardi.service.storage.driver.elastic.profile import load_profiles_for_auto_merge
from tracardi.service.storage.redis.collections import Collection
from tracardi.service.storage.redis_client import RedisClient
from tracardi.service.tracking.locking import GlobalMutexLock
from tracardi.service.utils.getters import get_entity_id

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

print(f"TRACARDI version {tracardi.version}")


async def worker():
    logger.info("Starting single tenant auto profile merging worker...")
    no_of_profiles = 0
    async for profile_record in load_profiles_for_auto_merge():
        profile = profile_record.to_entity(Profile)
        no_of_profiles += 1
        _redis = RedisClient()
        async with (
            GlobalMutexLock(get_entity_id(profile),
                            'profile',
                            namespace=Collection.lock_tracker,
                            redis=_redis,
                            name='profile_merging_worker',
                            lock_ttl=5
                            )):
            await deduplicate_profile(profile.id, profile.ids)
    logger.info(f"Merged {no_of_profiles} ...")
    logger.info("No more profiles to merge. Merging finished ...")


async def main():
    production = os.environ.get('PRODUCTION', 'no') == 'yes'
    tenant = os.environ.get('TENANT', tracardi.version.name)

    await wait_for_connection()

    context = Context(production=production, tenant=tenant)
    with ServerContext(context):
        if config.mode == 'job':
            await worker()
        else:
            while True:
                await worker()
                logger.info(f"Pausing for {config.pause}s ...")
                await sleep(config.pause)


MainLoop(main)
