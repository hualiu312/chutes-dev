import asyncio
import json
import sys
import traceback
from datetime import datetime

from loguru import logger

sys.path.append("..")

from gepetto2 import ClusterManager, LocalManager


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


def test_cases():

    async def _get_nodes():
        nodes = await ClusterManager.get_nodes()
        assert nodes is not None
        logger.success(f"Got cluster nodes: {len(nodes)} ")
        print(json.dumps(nodes, indent=2, skipkeys=True, cls=DateTimeEncoder))

    async def _get_pods():
        pods = await ClusterManager.get_pods()
        assert pods is not None
        logger.success(f"Got cluster pods: {len(pods)}")
        print(json.dumps(pods, indent=2, skipkeys=True, cls=DateTimeEncoder))

    async def _get_deployed_chutes():
        chutes = await ClusterManager.get_deployed_chutes()
        assert chutes is not None
        logger.success(f"Got deployed chutes: {len(chutes)}")

        print(json.dumps(chutes, indent=2, skipkeys=True, cls=DateTimeEncoder))

    async def _get_instances():

        insts = await ClusterManager.get_deployed_chutes()
        assert insts is not None

        for inst in insts:
            chute = await LocalManager.get_chute(inst["chute_id"])
            inst["chute_name"] = chute.name if chute else None

        insts = list(filter(lambda i: i["chute_name"], insts))

        print(json.dumps(insts, indent=2, skipkeys=True, cls=DateTimeEncoder))

    async def _get_config_ids():
        pods = await ClusterManager.get_pods(label_selector="chutes/config-id")
        print(json.dumps(pods, indent=2, skipkeys=True, cls=DateTimeEncoder))
        for p in pods:
            print(p["labels"]["chutes/config-id"])

    # -----------------------------------------
    return [
        _get_nodes,
        _get_pods,
        _get_deployed_chutes,
        _get_instances,
        _get_config_ids,
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
