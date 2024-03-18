import logging
import os
from asyncio import sleep

from aio.loop import MainLoop

from config import config
from tracardi.config import tracardi
from tracardi.context import ServerContext, Context, get_context
from tracardi.domain.profile import Profile
from tracardi.service.elastic.connection import wait_for_connection
from tracardi.service.license import License, LICENSE
from tracardi.service.profile_deduplicator import deduplicate_profile
from tracardi.service.storage.driver.elastic.profile import load_profiles_for_auto_merge, \
    load_profiles_with_duplicated_ids, load_by_ids
from tracardi.service.storage.redis.collections import Collection
from tracardi.service.storage.redis_client import RedisClient
from tracardi.service.tracking.locking import async_mutex, Lock
from tracardi.service.utils.getters import get_entity_id

if License.has_service(LICENSE):
    from com_tracardi.service.multi_tenant_manager import MultiTenantManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

print(f"TRACARDI version {tracardi.version}")


async def _deduplicate(_redis, profile_record):
    try:
        profile = profile_record.to_entity(Profile)
        key = Lock.get_key(Collection.lock_tracker, "profile", get_entity_id(profile))
        lock = Lock(_redis, key, default_lock_ttl=5)
        async with async_mutex(lock, name='profile_merging_worker'):
            await deduplicate_profile(profile.id, profile.ids)
    except Exception as e:
        logger.error(f"Error for profile {profile_record}: {str(e)}")


async def worker():
    try:
        logger.info(f"Merging in context {get_context()}...")
        no_of_profiles = 0
        _redis = RedisClient()

        profile_ids = {profile_id async for profile_id in load_profiles_with_duplicated_ids()}
        logger.info(f"Found {len(profile_ids)} duplicated ids in context {get_context()}...")

        async for profile_record in load_profiles_for_auto_merge():
            no_of_profiles += 1
            await _deduplicate(_redis, profile_record)

        async for profile_record in load_by_ids(list(profile_ids), batch=1000):
            no_of_profiles += 1
            await _deduplicate(_redis, profile_record)

        logger.info(f"Merged {no_of_profiles} ...")
        logger.info("No more profiles to merge. Merging finished ...")
    except Exception as e:
        logger.error(f"Error: {str(e)}")


async def start_worker(tenant: str):
    with ServerContext(Context(production=True, tenant=tenant)):
        await worker()
    with ServerContext(Context(production=False, tenant=tenant)):
        await worker()


async def _main():
    try:
        if License.has_service(LICENSE) and tracardi.multi_tenant:
            logger.info(f"Starting multi tenant auto profile merging worker in mode {config.mode}...")
            tms = MultiTenantManager()
            if not tracardi.multi_tenant_manager_api_key:
                raise ConnectionError("TMS URL or API_KEY not defined.")
            await tms.authorize(tracardi.multi_tenant_manager_api_key)
            logger.info(f"Loading tenants form {tms.tenants_endpoint}...")
            tenants = [tenant async for tenant in tms.list_tenants()]

            logger.info(f"Found {len(tenants)} tenants...")
            for tenant in tenants:
                logger.info(f"Running tenant `{tenant.id}`...")
                await start_worker(tenant.id)
        else:
            logger.info("Starting single tenant auto profile merging worker...")
            tenant = os.environ.get('TENANT', tracardi.version.name)
            await start_worker(tenant)
    except Exception as e:
        logger.info(f"Error: {str(e)}...")

async def main():
    if config.mode == 'job':
        await _main()
    else:
        while True:
            await _main()
            logger.info(f"Pausing for {config.pause}s ...")
            await sleep(config.pause)


MainLoop(main)
