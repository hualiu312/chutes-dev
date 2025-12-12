import asyncio
import sys
import traceback
import uuid
from loguru import logger
from chutes_miner.api.config import settings
from chutes_common.schemas.chute import Chute

sys.path.append("..")

from gepetto2 import LocalManager


def test_cases():

    _VLD_KEY = settings.validators[0].hotkey

    async def _get_chutes():

        chute_lst = await LocalManager.get_chutes(_VLD_KEY)
        assert chute_lst is not None
        logger.success(f"Got local chutes: {len(chute_lst)}")

    async def _get_chute():
        chute_lst = await LocalManager.get_chutes(_VLD_KEY)
        assert len(chute_lst) > 0
        chute_id = chute_lst[0].chute_id
        assert chute_id is not None

        chute_info = await LocalManager.get_chute(chute_id)
        assert chute_info is not None
        logger.success(f"Got chute by ID: {chute_info.chute_id=}, {chute_info.name=}")

    async def _create_delete_chute():
        chute_id = str(uuid.uuid4())
        ori_info = {
            "validator": _VLD_KEY,
            "chute_id": chute_id,
            "version": str(uuid.uuid4()),
            "name": "test-chute",
            "image": "chutes/bert-sentiment-pipeline:0.0.1",
            "code": "\npass\n",
            "filename": "chute_file.py",
            "ref_str": "chute_file:chute",
            "gpu_count": 4,
            "supported_gpus": ["l4", "a5000", "h100", "4090", "a10", "3090"],
            "chutes_version": "0.9.9",
            "preemptible": False,
        }
        chute1 = Chute(**ori_info)
        await LocalManager.create_chute(chute1)
        logger.success(f"Created chute: {chute1.chute_id=}, {chute1.name=}")

        chute2 = await LocalManager.get_chute(chute_id)
        assert chute2 is not None
        assert chute2.name == ori_info["name"]
        assert chute2.code == ori_info["code"]
        assert chute2.filename == ori_info["filename"]
        assert chute2.image == ori_info["image"]
        assert chute2.gpu_count == ori_info["gpu_count"]
        assert chute2.supported_gpus == ori_info["supported_gpus"]
        assert chute2.preemptible == ori_info["preemptible"]
        assert chute2.ref_str == ori_info["ref_str"]
        assert chute2.version == ori_info["version"]
        assert chute2.chutes_version == ori_info["chutes_version"]

        await LocalManager.delete_chute(chute_id)
        chute3 = await LocalManager.get_chute(chute_id)
        assert chute3 is None
        logger.success(f"Deleted chute: {chute2.chute_id=}, {chute2.name=}")

    async def _create_update_chute():
        chute_id = str(uuid.uuid4())
        ori_info = {
            "validator": _VLD_KEY,
            "chute_id": chute_id,
            "version": str(uuid.uuid4()),
            "name": "test-chute",
            "image": "chutes/bert-sentiment-pipeline:0.0.1",
            "code": "\npass\n",
            "filename": "chute_file.py",
            "ref_str": "chute_file:chute",
            "gpu_count": 4,
            "supported_gpus": ["l4", "a5000", "h100", "4090", "a10", "3090"],
            "chutes_version": "0.9.9",
            "preemptible": False,
        }
        chute1 = Chute(**ori_info)
        await LocalManager.create_chute(chute1)
        logger.success(f"Created chute: {chute1.chute_id=}, {chute1.name=}")

        upd_vals = {
            "code": "\npass\npass\n",
            "image": "chute_image2",
            "filename": "chute_file2.py",
            "ref_str": "chute_file2:chute",
            "gpu_count": 8,
            "supported_gpus": ["5090", "a100", "h100", "h800"],
            "preemptible": True,
            "version": "v2",
            "chutes_version": "v2",
        }
        await LocalManager.update_chute(chute_id, upd_vals)
        chute2 = await LocalManager.get_chute(chute_id)
        assert chute2 is not None
        assert chute2.code == upd_vals["code"]
        assert chute2.image == upd_vals["image"]
        assert chute2.filename == upd_vals["filename"]
        assert chute2.ref_str == upd_vals["ref_str"]
        assert chute2.gpu_count == upd_vals["gpu_count"]
        assert chute2.supported_gpus == upd_vals["supported_gpus"]
        assert chute2.preemptible == upd_vals["preemptible"]
        assert chute2.version == upd_vals["version"]
        assert chute2.chutes_version == upd_vals["chutes_version"]

        await LocalManager.delete_chute(chute_id)
        chute3 = await LocalManager.get_chute(chute_id)
        assert chute3 is None
        logger.success(f"Deleted chute: {chute2.chute_id=}, {chute2.name=}")

    async def _get_deployments():
        depls = await LocalManager.get_deployments(_VLD_KEY)
        assert depls is not None
        logger.success(f"Got local deployments: {len(depls)}")

    async def _get_deployment():
        depl_lst = await LocalManager.get_deployments(_VLD_KEY)
        assert len(depl_lst) > 0
        depl_id = depl_lst[0].deployment_id
        depl = await LocalManager.get_deployment(depl_id)
        assert depl is not None
        logger.success(f"Got local deployment: {depl.deployment_id=}, {depl.chute_id=}, {depl.instance_id=}")
        logger.info(f"Deployment: {depl.to_dict()}")

    # -----------------------------------------
    return [
        _get_chutes,
        _get_chute,
        _create_delete_chute,
        _create_update_chute,
        _get_deployments,
        _get_deployment,
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
