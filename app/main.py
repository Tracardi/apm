import logging
import os

from app.aio.loop import MainLoop
from tracardi.config import tracardi
from tracardi.context import ServerContext, Context
from tracardi.domain.profile import Profile
from tracardi.service.elastic.connection import wait_for_connection
from tracardi.service.profile_deduplicator import deduplicate_profile
from tracardi.service.storage.driver.elastic.profile import load_profiles_for_auto_merge

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

print(f"TRACARDI version {tracardi.version}")


async def main():
    production = os.environ.get('PRODUCTION', 'no') == 'yes'
    tenant = os.environ.get('TENANT', tracardi.version.name)

    await wait_for_connection()

    logger.info("Starting single tenant auto profile merging worker...")
    context = Context(production=production, tenant=tenant)
    with ServerContext(context):
        records = await load_profiles_for_auto_merge()
        for profile in records.to_domain_objects(Profile):
            await deduplicate_profile(profile.id, profile.ids)


MainLoop(main)
