import asyncio
import sys
import traceback
from loguru import logger
from chutes_miner.api.config import settings
from chutes_common.schemas.deployment import Deployment
from chutes_common.schemas.gpu import GPU

sys.path.append("..")

from gepetto import RemoteApi


def test_cases():

    _VLD_KEY = settings.validators[0].hotkey

    _test_chute_id = "59ea125d-f4fc-5ee2-b873-433d4e3a49dd"
    _test_chute_ver = "28642b6a-fec8-54d8-b56e-f15066b08185"

    async def _get_chutes():
        chutes = await RemoteApi.get_chutes(_VLD_KEY)
        assert chutes is not None
        logger.success(f"Got remote chutes: {len(chutes)} ")

        items = list(chutes.values())
        items.sort(key=lambda x: x["created_at"], reverse=True)

        chute = items[0]
        print(chute)

    async def _get_images():
        images = await RemoteApi.get_images(_VLD_KEY)
        assert images is not None
        logger.success(f"Got remote images: {len(images)}")

    async def _get_instances():
        instances = await RemoteApi.get_instances(_VLD_KEY)
        assert instances is not None
        logger.success(f"Got remote instances: {len(instances)}")

    async def _get_metrics():
        metrics = await RemoteApi.get_metrics(_VLD_KEY)
        assert metrics is not None
        logger.success(f"Got remote metrics: {len(metrics)}")

    async def _get_nodes():
        nodes = await RemoteApi.get_nodes(_VLD_KEY)
        assert nodes is not None
        logger.success(f"Got remote nodes: {len(nodes)}")

    async def _get_utilization():
        logger.info("Test RemoteApi.get_utilization")
        utils = await RemoteApi.get_utilization(_VLD_KEY)
        assert utils is not None
        logger.success(f"Got utilization: {len(utils)}")

    async def _get_rolling_updates():
        updates = await RemoteApi.get_rolling_updates(_VLD_KEY)
        assert updates is not None
        logger.success(f"Got rolling updates: {len(updates)}")

    async def _get_chute():
        chute = await RemoteApi.get_chute(_VLD_KEY, _test_chute_id, _test_chute_ver)
        assert chute is not None
        logger.success(f"Got remote chute: id={chute['chute_id']} name={chute['name']} image={chute['image']}")

    async def _get_launch_token():
        token = await RemoteApi.get_launch_token(_VLD_KEY, _test_chute_id)
        assert token is not None
        logger.success(f"Got launch token: {token}")

    async def _purge_instance():
        chute_id = "00cf77cb-8720-5d68-bdfc-299af37363d3"
        instance_id = "01f0c0f5-c0c9-5d05-b0c9-01f0c0f5c0c9"
        res = await RemoteApi.purge_instance(_VLD_KEY, chute_id, instance_id)
        assert res is not None
        logger.success(f"Purged instance: {res}")

    async def _post_deployment():
        deployment = Deployment(
            validator=_VLD_KEY,
            chute_id="00cf77cb-8720-5d68-bdfc-299af37363d3",
            host="10.8.8.6",
            port=30300,
            gpus=[GPU(gpu_id="2d6b762842e9f38dc18bb02d7aa913dc")],
        )
        res = await RemoteApi.post_deployment(deployment)
        logger.info(f"Posted deployment: {res}")

    # -----------------------------------------
    return [
        _get_chutes,
        _get_images,
        _get_instances,
        _get_metrics,
        _get_nodes,
        _get_utilization,
        _get_rolling_updates,
        _get_chute,
        _get_launch_token,
        _purge_instance,
        _post_deployment,
    ]


async def run_tests(test_name: str = None, stop_on_error: bool = True):

    logger.info(f"Testing {__file__} ...")

    cases = list(filter(lambda x: test_name in x.__name__, test_cases())) if test_name else test_cases()

    tot = len(cases)
    idx = 0
    psd = 0
    for item in cases:

        idx += 1
        name = item.__name__
        try:
            logger.info(f"Testing {idx}/{tot}: {name}")
            await item()
            psd += 1
        except Exception as ex:
            logger.error(f"{name} failed: {ex}\n{traceback.format_exc()}")
            if stop_on_error:
                break

    logger.info(f"Total {tot} tests: {idx} tested, {psd} passed.")


if __name__ == "__main__":
    asyncio.run(run_tests(sys.argv[1] if len(sys.argv) > 1 else None))

# copy to gepetto pod：
# cd /d %~dp0
# scp curr_file.py cpu0:~/chutes-dev/
# ssh cpu0 "p=$(kubectl get pods -l app.kubernetes.io/name=gepetto -o jsonpath='{.items[0].metadata.name}') && kubectl cp ~/chutes-dev/curr_file.py $p:/app/"
# exec sh gepetto pod：
# python curr_file.py
