import os
from asyncio import sleep

from aio.loop import MainLoop

from config import config
from tracardi.config import tracardi
from tracardi.context import ServerContext, Context, get_context
from tracardi.domain import ExtraInfo
from tracardi.domain.profile import Profile
from tracardi.exceptions.exception import StorageException
from tracardi.exceptions.log_handler import get_logger
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

logger = get_logger(__name__)

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
        logger.info(f"Merging in context {get_context()}...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
        no_of_profiles = 0
        _redis = RedisClient()

        profile_ids = {profile_id async for profile_id in load_profiles_with_duplicated_ids(log_error=False)}
        logger.info(f"Found {len(profile_ids)} duplicated ids in context {get_context()}...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))

        async for profile_record in load_profiles_for_auto_merge():
            no_of_profiles += 1
            await _deduplicate(_redis, profile_record)

        async for profile_record in load_by_ids(list(profile_ids), batch=1000):
            no_of_profiles += 1
            await _deduplicate(_redis, profile_record)

        logger.info(f"Merged {no_of_profiles} ...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
        logger.info("No more profiles to merge. Merging finished ...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
    except StorageException as e:
        logger.warning(f"Error while loading profiles. Is system installed?. Details: {str(e)}", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
    except Exception as e:
        logger.error(f"Error: {str(e)}", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))


async def start_worker(tenant: str):
    with ServerContext(Context(production=True, tenant=tenant)):
        await worker()
    with ServerContext(Context(production=False, tenant=tenant)):
        await worker()


async def _main():
    await wait_for_connection(no_of_tries=10)

    try:
        if License.has_service(LICENSE) and tracardi.multi_tenant:
            logger.info(f"Starting multi tenant auto profile merging worker in mode {config.mode}...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
            tms = MultiTenantManager()
            if not tracardi.multi_tenant_manager_api_key:
                raise ConnectionError("TMS URL or API_KEY not defined.")
            await tms.authorize(tracardi.multi_tenant_manager_api_key)
            logger.info(f"Loading tenants form {tms.tenants_endpoint}...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
            tenants = [tenant async for tenant in tms.list_tenants()]

            logger.info(f"Found {len(tenants)} tenants...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
            for tenant in tenants:
                logger.info(f"Running tenant `{tenant.id}`...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
                await start_worker(tenant.id)
        else:
            logger.info("Starting single tenant auto profile merging worker...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
            tenant = os.environ.get('TENANT', tracardi.version.name)
            await start_worker(tenant)
    except Exception as e:
        logger.info(f"Error: {str(e)}...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))


async def main():
    if config.mode == 'job':
        await _main()
    else:
        while True:
            await _main()
            logger.info(f"Pausing for {config.pause}s ...", extra=ExtraInfo.exact(origin="APM", package=__name__, class_name="worker"))
            await sleep(config.pause)


MainLoop(main)
