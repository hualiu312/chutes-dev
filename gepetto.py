"""
Gepetto - coordinate all the things.
V251115：
* 命令选项：python gepetto.py [command] --args
  + chutes | scales
  + deploy | undeploy
  + recycle
* 自动部署，限定模型白名单：_CANDIDATE_CHUTES
  仅允许存在于白名单的匹配模型才能部署
* 失败回避策略，避免重复部署同一个排名最高但已失败的模型：
  + 记录模型实例创建和删除时间，前后时间间隔太短的，则认为部署失败
  + 在白名单排名列表中，优先选择未被标记为“失败”的最高分模型
  + 若所有候选模型均失败，则选择其中最早被删除的模型
"""

# 候选 Chutes
_CANDIDATE_CHUTES = []

# region imports

import argparse
import aiohttp
import asyncio
import hashlib
import traceback
import inspect
import orjson as json
import sys

from datetime import datetime, timedelta, timezone
from loguru import logger
from sqlalchemy import func, delete, select, text, update
from sqlalchemy.orm import selectinload
from typing import Any, Callable, Dict, Optional
from chutes_common.auth import sign_request
from chutes_common.schemas import Base
from chutes_common.schemas.chute import Chute
from chutes_common.schemas.deployment import Deployment
from chutes_common.schemas.server import Server
from chutes_common.schemas.gpu import GPU
from chutes_miner.api.config import settings, validator_by_hotkey
from chutes_miner.api.database import engine, get_session
from chutes_miner.api.k8s.operator import K8sOperator
from chutes_miner.api.redis_pubsub import RedisListener
import chutes_miner.api.k8s as k8s

# endregion

# region logging


def log_filter(rec):
    if rec["name"] == "chutes_miner.api.redis_pubsub":
        return rec["level"].no >= 30
    elif rec["name"] == "__main__":
        return rec["level"].no >= 10
    else:
        return rec["level"].no >= 20


logger.remove()
logger.add(sys.stdout, filter=log_filter)

# endregion


class Gepetto:

    def __init__(self):

        self._chdepl = ChuteDeployer()
        self._rmsync = RemoteSynchro()

        self._scaler = Autoscaler(
            chute_deployer=self._chdepl,
            remote_synchro=self._rmsync,
        )
        self._recler = Reconciler(
            chute_deployer=self._chdepl,
            remote_synchro=self._rmsync,
        )

        self._pubsub = RedisListener()
        self._on_events()

    def _on_events(self):

        self._pubsub.on_event("gpu_verified")(self._gpu_verified)
        self._pubsub.on_event("gpu_deleted")(self._gpu_deleted)
        self._pubsub.on_event("server_deleted")(self._server_deleted)

        self._pubsub.on_event("chute_created")(self._chute_created)
        self._pubsub.on_event("chute_deleted")(self._chute_deleted)
        self._pubsub.on_event("chute_updated")(self._chute_updated)

        # self._pubsub.on_event("image_created")(self._image_created)
        # self._pubsub.on_event("image_deleted")(self._image_deleted)
        self._pubsub.on_event("image_updated")(self._image_updated)

        self._pubsub.on_event("instance_created")(self._instance_created)
        self._pubsub.on_event("instance_deleted")(self._instance_deleted)
        self._pubsub.on_event("instance_verified")(self._instance_verified)
        self._pubsub.on_event("instance_activated")(self._instance_activated)

        self._pubsub.on_event("job_created")(self._job_created)
        self._pubsub.on_event("job_deleted")(self._job_deleted)

        self._pubsub.on_event("rolling_update")(self._rolling_update)
        # self._pubsub.on_event("bounty_changed")(self._bounty_changed)

    async def _dbsync(self):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def run(self):

        await self._dbsync()
        await self._rmsync.synchronize()
        await self._recler.reconcile()
        asyncio.create_task(self._rmsync.run())
        asyncio.create_task(self._scaler.run())
        asyncio.create_task(self._recler.run())
        await self._pubsub.start()

    # region server gpu events

    async def _gpu_verified(self, event_data: dict):
        logger.info(f"Received gpu_verified event: {event_data}")
        gpu_id = event_data["gpu_id"]
        await LocalManager.update_gpu(gpu_id=gpu_id, values={"verified": True})

    async def _gpu_deleted(self, event_data: dict):
        logger.info(f"Received gpu_deleted event: {event_data}. !TODO!")
        gpu_id = event_data["gpu_id"]
        # TODO: 删除 GPU

    async def _server_deleted(self, event_data: dict):
        logger.info(f"Received server_deleted event: {event_data}. !TODO!")
        server_id = event_data["server_id"]
        # TODO: 删除 Server

    # endregion

    # region chute events

    async def _chute_created(self, event_data: dict):
        logger.info(f"Received chute_created event: {event_data}. !TODO!")
        # TODO: 创建 Chute

    async def _chute_deleted(self, event_data: dict):
        logger.info(f"Received chute_deleted event: {event_data}. !TODO!")
        # TODO: 删除 Chute

    async def _chute_updated(self, event_data: dict):
        logger.info(f"Received chute_updated event: {event_data}. !TODO!")
        # TODO: 更新 Chute

    # endregion

    # region image events

    async def _image_created(self, event_data: dict):
        logger.info(f"Received image_created event: {event_data}. !TODO!")

    async def _image_deleted(self, event_data: dict):
        logger.info(f"Received image_deleted event: {event_data}. !TODO!")

    async def _image_updated(self, event_data: dict):
        logger.info(f"Received image_updated event: {event_data}. !TODO!")

    # endregion

    # region instance events

    async def _instance_created(self, event_data: dict):
        logger.info(f"Received instance_created event: {event_data}")
        if event_data["miner_hotkey"] != settings.miner_ss58:
            return

        config_id = event_data["config_id"]
        instance_id = event_data["instance_id"]
        await LocalManager.update_deployment(config_id=config_id, values={"instance_id": instance_id})

    async def _instance_verified(self, event_data: dict):
        logger.info(f"Received instance_verified event: {event_data}")
        if event_data["miner_hotkey"] != settings.miner_ss58:
            return

        instance_id = event_data["instance_id"]
        await LocalManager.update_deployment(instance_id=instance_id, values={"verified_at": func.now()})

    async def _instance_activated(self, event_data: dict):
        logger.info(f"Received _instance_activated event: {event_data}")
        if event_data["miner_hotkey"] != settings.miner_ss58:
            return

        instance_id = event_data["instance_id"]
        await LocalManager.update_deployment(
            instance_id=instance_id, values={"stub": False, "active": True, "activated_at": func.now()}
        )

    async def _instance_deleted(self, event_data: dict):
        logger.info(f"Received instance_deleted event: {event_data}")
        if event_data["miner_hotkey"] != settings.miner_ss58:
            return

        instance_id = event_data["instance_id"]
        await self._chdepl.undeploy(instance_id)

    # endregion

    # region job events

    async def _job_created(self, event_data: dict):
        logger.info(f"Received job_created event: {event_data}. !TODO!")

    async def _job_deleted(self, event_data: dict):
        logger.info(f"Received job_deleted event: {event_data}. !TODO!")

    # endregion

    # region rolling update events

    async def _rolling_update(self, event_data: dict):
        logger.info(f"Received rolling_update event: {event_data}. !TODO!")

    # endregion

    # region bounty events

    async def _bounty_changed(self, event_data: dict):
        logger.info(f"Received bounty_changed event: {event_data}. !TODO!")

    # endregion


class RemoteApi:

    @staticmethod
    def _api_addr(validator_key: str):
        vld = validator_by_hotkey(validator_key)
        return vld.api

    @staticmethod
    async def _resp_err(resp: aiohttp.ClientResponse):
        txt = await resp.text(encoding="utf-8", errors="replace")
        msg = f"{resp.method} {resp.url} failed {resp.reason} [{resp.status}]\n"
        msg += txt or "<no content>"
        logger.error(msg)
        resp.raise_for_status()

    @staticmethod
    async def load_remote_objects(
        val_key: str, obj_name: str, key_name: str, params: dict | None = None
    ) -> dict[str, dict]:
        """
        从验证器获取指定类型的资源（如 chutes、images、instances、nodes、metrics 等）
        注：通过SSE（Server-Sent Events）接收资源数据。
        """
        async with aiohttp.ClientSession(read_bufsize=10 * 1024 * 1024) as session:

            url = f"{RemoteApi._api_addr(val_key)}/miner/{obj_name}/"
            hdr, _ = sign_request(purpose="miner")  # 生成请求签名
            par = params or {}

            items = {}
            async with session.get(url, headers=hdr, params=par) as resp:

                if resp.ok:
                    # 读取SSE响应内容
                    async for content_enc in resp.content:
                        content = content_enc.decode()
                        if content.startswith("data: {"):
                            # 解析资源数据（SSE格式为"data: {json}"）
                            data = json.loads(content[6:])
                            items[data[key_name]] = data
                        elif content.startswith("data: NO_ITEMS"):
                            # 明确返回无资源
                            # explicit_null = True
                            continue
                        else:
                            if content.strip() != "":
                                raise Exception(f"Unexpected content: {content}")
                else:
                    await RemoteApi._resp_err(resp)
            return items

    @staticmethod
    async def get_chutes(val_key: str) -> dict[str, dict]:
        obj = "chutes"
        key = "chute_id"
        items = await RemoteApi.load_remote_objects(val_key, obj, key)
        return items

    @staticmethod
    async def get_images(val_key: str) -> dict[str, dict]:
        obj = "images"
        key = "image_id"
        items = await RemoteApi.load_remote_objects(val_key, obj, key)
        return items

    @staticmethod
    async def get_instances(val_key: str) -> dict[str, dict]:
        obj = "instances"
        key = "instance_id"
        par = {"explicit_null": "True"}
        items = await RemoteApi.load_remote_objects(val_key, obj, key, par)
        return items

    @staticmethod
    async def get_metrics(val_key: str) -> dict[str, dict]:
        obj = "metrics"
        key = "chute_id"
        items = await RemoteApi.load_remote_objects(val_key, obj, key)
        return items

    @staticmethod
    async def get_nodes(val_key: str) -> dict[str, dict]:
        obj = "nodes"
        key = "uuid"
        items = await RemoteApi.load_remote_objects(val_key, obj, key)
        return items

    @staticmethod
    async def get_utilization(val_key: str) -> dict[str, dict]:
        async with aiohttp.ClientSession() as session:
            url = f"{RemoteApi._api_addr(val_key)}/chutes/utilization"
            items = {}
            async with session.get(url) as resp:
                if resp.ok:
                    data = await resp.json()
                    for it in data:
                        items[it["chute_id"]] = it
                else:
                    await RemoteApi._resp_err(resp)

            return items

    @staticmethod
    async def get_rolling_updates(val_key: str):
        async with aiohttp.ClientSession() as session:
            url = f"{RemoteApi._api_addr(val_key)}/chutes/rolling_updates"
            items = {}
            async with session.get(url) as resp:
                if resp.ok:
                    data = await resp.json()
                    for it in data:
                        items[it["chute_id"]] = it
                else:
                    await RemoteApi._resp_err(resp)
            return items

    @staticmethod
    async def get_chute(val_key: str, chute_id: str, version: str) -> dict:
        async with aiohttp.ClientSession() as session:
            url = f"{RemoteApi._api_addr(val_key)}/miner/chutes/{chute_id}/{version}"
            hdr, _ = sign_request(purpose="miner")
            async with session.get(url, headers=hdr) as resp:
                if resp.ok:
                    data = await resp.json()
                    return data
                elif resp.status == 404:
                    return None
                else:
                    await RemoteApi._resp_err(resp)

    @staticmethod
    async def get_launch_token(val_key: str, chute_id: str, job_id: str = None) -> dict:
        async with aiohttp.ClientSession() as session:
            url = f"{RemoteApi._api_addr(val_key)}/instances/launch_config"
            hdr, _ = sign_request(purpose="launch")
            par = {"chute_id": chute_id}
            if job_id:
                par["job_id"] = job_id

            async with session.get(url, headers=hdr, params=par) as resp:
                if resp.ok:
                    data = await resp.json()
                    return data
                else:
                    await RemoteApi._resp_err(resp)

    @staticmethod
    async def purge_instance(val_key: str, chute_id: str, instance_id: str) -> bool:
        async with aiohttp.ClientSession() as session:
            url = f"{RemoteApi._api_addr(val_key)}/instances/{chute_id}/{instance_id}"
            hdr, _ = sign_request(purpose="instances")
            async with session.delete(url, headers=hdr) as resp:
                if resp.ok or resp.status == 404:
                    return True
                else:
                    await RemoteApi._resp_err(resp)

    @staticmethod
    async def post_deployment(deployment: Deployment) -> dict:

        async with aiohttp.ClientSession() as session:
            val = deployment.validator
            url = f"{RemoteApi._api_addr(val)}/instances/{deployment.chute_id}/"
            bdy = {
                "node_ids": [gpu.gpu_id for gpu in deployment.gpus],
                "host": deployment.host,
                "port": deployment.port,
            }
            hdr, pld = sign_request(payload=bdy)
            async with session.post(url, headers=hdr, data=pld) as resp:
                if resp.ok:
                    obj = await resp.json()
                    return obj
                else:
                    await RemoteApi._resp_err(resp)


class ClusterManager:

    @staticmethod
    async def get_nodes() -> list[dict]:
        """
        { "name": "chutes-miner-gpu-1",
          "validator": "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
          "server_id": "20c444a2-98f8-4153-8e66-4aa9885ee3d6",
          "status": null,
          "ip_address": "39.109.84.2",
          "cpu_per_gpu": 4,
          "memory_gb_per_gpu": 24,
          "disk_total_gb": 936.7875633239746,
          "disk_available_gb": 916.7875633239746,
          "disk_used_gb": 0 }
        """
        nodes = await K8sOperator().get_kubernetes_nodes()
        return nodes

    @staticmethod
    async def get_pods(namespace: str | None = None, label_selector: str | dict | None = None) -> list[dict]:

        def _to_pod_info(pod) -> dict:
            return {
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
                "status": pod.status.phase if pod.status else None,
                "node_name": pod.spec.node_name if pod.spec else None,
                "pod_ip": pod.status.pod_ip if pod.status else None,
                "host_ip": pod.status.host_ip if pod.status else None,
                "container_name": " , ".join([container.name for container in pod.spec.containers]),
                "image_name": " , ".join([container.image for container in pod.spec.containers]),
                "labels": pod.metadata.labels or {},
                "start_time": pod.status.start_time.isoformat() if pod.status and pod.status.start_time else None,
            }

        plst = K8sOperator().get_pods(namespace, label_selector)
        pods = [_to_pod_info(pod) for pod in plst.items]
        return pods

    @staticmethod
    async def get_deployed_chutes() -> list[dict]:

        def _as_status(sta) -> str:
            if sta:
                if sta.get("failed", 0) > 0:
                    return "Failed"
                if sta.get("succeeded", 0) > 0:
                    return "Succeeded"
                if sta.get("active", 0) > 0:
                    return "Active"
            return "None"

        def _get_pod(pods) -> dict:
            if len(pods) > 0:
                pod = pods[0]
                res = None
                msg = None
                if sta := pod.get("state"):
                    if det := sta.get("waiting") or sta.get("terminated"):
                        res = det.get("reason")
                        msg = det.get("message")

                return {"pod_name": pod.get("name"), "pod_phase": pod.get("phase"), "reason": res, "message": msg}
            else:
                return {"pod_name": None, "pod_phase": None, "reason": None, "message": None}

        depls = await k8s.get_deployed_chutes()
        insts = []
        for d in depls:
            inf = {
                "name": d["name"],
                "node_name": d["node"],
                "chute_id": d["chute_id"],
                "version": d["version"],
                "config_id": d["labels"].get("chutes/config-id"),
                "deployment_id": d["deployment_id"],
                "ready": d["ready"],
                "status": _as_status(d["status"]),
                "start_time": d["status"].get("start_time"),
            }
            pod = _get_pod(d["pods"])
            insts.append({**inf, **pod})

        return insts

    @staticmethod
    async def get_deployment(depl_id: str) -> dict:
        depl = await k8s.get_deployment(depl_id)
        return depl

    @staticmethod
    async def deploy_chute(chute_id: str, server_id: str, token: str, config_id: str) -> Deployment:
        depl, _ = await k8s.deploy_chute(chute_id, server_id, token, config_id)
        return depl

    @staticmethod
    async def undeploy_chute(deployment_id: str) -> None:
        await k8s.undeploy(deployment_id)

    @staticmethod
    async def check_disk_available(node_name: str, required_disk_gb: int) -> bool:
        avail = await k8s.check_node_has_disk_available(node_name, required_disk_gb)
        return avail

    @staticmethod
    async def create_code_config_map(chute: Chute):
        await k8s.create_code_config_map(chute)

    @staticmethod
    async def delete_code_config_map(chute_id: str, version: str) -> None:
        await k8s.delete_code(chute_id, version)


class LocalManager:

    @staticmethod
    async def get_chutes(validator: str) -> list[Chute]:
        async with get_session() as session:
            return (await session.execute(select(Chute).where(Chute.validator == validator))).unique().scalars().all()

    @staticmethod
    async def get_chute(chute_id: str) -> Optional[Chute]:
        async with get_session() as session:
            return (await session.execute(select(Chute).where(Chute.chute_id == chute_id))).scalar_one_or_none()

    @staticmethod
    async def create_chute(chute) -> None:
        async with get_session() as session:
            session.add(chute)
            await session.commit()
            await ClusterManager.create_code_config_map(chute)

    @staticmethod
    async def update_chute(chute_id: str, values: dict[str, Any]) -> None:
        if not values:
            return

        async with get_session() as session:
            await session.execute(update(Chute).where(Chute.chute_id == chute_id).values(values))
            await session.commit()
            # TODO: update code config map

    @staticmethod
    async def delete_chute(chute_id: str):
        async with get_session() as session:
            chute = (
                await session.execute(
                    select(Chute).where(Chute.chute_id == chute_id).options(selectinload(Chute.deployments))
                )
            ).scalar_one_or_none()
            if not chute:
                logger.warning(f"Chute {chute_id} not found")
                return

            if chute.deployments:
                deployer = ChuteDeployer()
                undeples = [deployer.undeploy(d.deployment_id) for d in chute.deployments]
                await asyncio.gather(*undeples)
            await session.delete(chute)
            await session.commit()
            await ClusterManager.delete_code_config_map(chute.chute_id, chute.version)

    @staticmethod
    async def get_deployments(validator: str) -> list[Deployment]:
        async with get_session() as session:
            return (
                (await session.execute(select(Deployment).where(Deployment.validator == validator)))
                .unique()
                .scalars()
                .all()
            )

    @staticmethod
    async def get_deployment(
        deployment_id: str = None, config_id: str = None, instance_id: str = None
    ) -> Optional[Deployment]:
        async with get_session() as session:

            if deployment_id:
                depl = (
                    (await session.execute(select(Deployment).where(Deployment.deployment_id == deployment_id)))
                    .unique()
                    .scalar_one_or_none()
                )

            elif config_id:
                depl = (
                    (await session.execute(select(Deployment).where(Deployment.config_id == config_id)))
                    .unique()
                    .scalar_one_or_none()
                )

            elif instance_id:
                depl = (
                    (await session.execute(select(Deployment).where(Deployment.instance_id == instance_id)))
                    .unique()
                    .scalar_one_or_none()
                )
            else:
                raise Exception("Primary ID required: deployment_id, config_id or instance_id")

            return depl

    @staticmethod
    async def update_deployment(
        deployment_id: str = None, config_id: str = None, instance_id: str = None, values: dict = {}
    ):
        if not (deployment_id or config_id or instance_id):
            raise Exception("Primary ID required: deployment_id, config_id or instance_id")
        if not values:
            return

        async with get_session() as session:

            if deployment_id:
                await session.execute(
                    update(Deployment).where(Deployment.deployment_id == deployment_id).values(values)
                )

            elif config_id:
                await session.execute(update(Deployment).where(Deployment.config_id == config_id).values(values))

            elif instance_id:
                await session.execute(update(Deployment).where(Deployment.instance_id == instance_id).values(values))

            await session.commit()

    @staticmethod
    async def delete_deployment(deployment_id: str = None, config_id: str = None, instance_id: str = None):
        async with get_session() as session:

            if deployment_id:
                await session.execute(delete(Deployment).where(Deployment.deployment_id == deployment_id))

            elif config_id:
                await session.execute(delete(Deployment).where(Deployment.config_id == config_id))

            elif instance_id:
                await session.execute(delete(Deployment).where(Deployment.instance_id == instance_id))

    @staticmethod
    async def get_server(server_id: str) -> Optional[Server]:
        pass

    @staticmethod
    async def delete_server(server_id: str):
        pass

    @staticmethod
    async def delete_gpu(gpu_id: str):
        pass


class ChuteDeployer:

    def __init__(self):
        self._chute_traces = {}

    async def deploy(self, chute_id: str, server_id: str, val_key: str | None = None) -> str:

        if val_key is None:
            val_key = settings.validators[0].hotkey

        if not (chute_id or server_id):
            raise Exception("Chute or server id is required")

        depl_id = None
        try:

            launch_token = await RemoteApi.get_launch_token(val_key, chute_id)
            deployment = await ClusterManager.deploy_chute(
                chute_id,
                server_id,
                token=launch_token["token"] if launch_token else None,
                config_id=launch_token["config_id"] if launch_token else None,
            )
            depl_id = deployment.deployment_id
            logger.success(f"Deployed chute ({chute_id}) on server ({server_id}): {depl_id=}")
            if not launch_token:
                await RemoteApi.post_deployment(deployment)

            self._trace_created(chute_id)

            return depl_id

        except Exception as ex:
            logger.error(f"Failed to deploy chute ({chute_id}) on server ({server_id}): {ex}\n{traceback.format_exc()}")

            if depl_id:
                await self.undeploy(depl_id)

            self._trace_deleted(chute_id)

    async def undeploy(self, deployment_id: str, instance_id: str | None = None) -> None:

        if not (deployment_id or instance_id):
            return

        try:
            depl = await LocalManager.get_deployment(deployment_id, instance_id=instance_id)
            if not depl:
                logger.warning(f"Deployment ({deployment_id=}, {instance_id=}) not found")
                return

            val_key = depl.validator
            chute_id = depl.chute_id
            deployment_id = depl.deployment_id
            instance_id = depl.instance_id

            await ClusterManager.undeploy_chute(deployment_id)
            await LocalManager.delete_deployment(deployment_id)
            await RemoteApi.purge_instance(val_key, chute_id, instance_id)
            self._trace_deleted(chute_id)

            logger.success(f"Undeploy chute ({chute_id}): {deployment_id=}, {instance_id=}")

        except Exception as ex:
            logger.error(f"Failed to undeploy chute ({deployment_id=}, {instance_id=}): {ex}\n{traceback.format_exc()}")

    @property
    def chute_traces(self) -> dict[str, dict]:
        self._trace_refresh()
        return self._chute_traces
        """
        { chute_id: { 
            created_at: datetime, 
            deleted_at: datetime, 
            failed: bool } 
        }
        """

    # region chute-trace

    def _trace_created(self, chute_id: str):
        if not chute_id:
            return
        trc = self._chute_traces.get(chute_id, {})
        trc["created_at"] = datetime.now()
        self._chute_traces[chute_id] = trc

    def _trace_deleted(self, chute_id: str):
        if not chute_id:
            return

        trc = self._chute_traces.get(chute_id, {})
        trc["deleted_at"] = datetime.now()
        if trc.get("created_at"):
            trc["failed"] = (trc["deleted_at"] - trc["created_at"]) <= timedelta(minutes=16)
        else:
            trc["created_at"] = None
            trc["failed"] = False
        self._chute_traces[chute_id] = trc

    def _trace_refresh(self):
        now = datetime.now()
        for trc in self._chute_traces.values():
            fld = trc.get("failed")
            dlt = trc.get("deleted_at")
            if fld and dlt and now - dlt > timedelta(hours=1):
                trc["failed"] = False

    # endregion


class RemoteSynchro:

    def __init__(
        self,
        validator_key: str = None,
        loop_interval: int = 4 * 60,
    ):
        self._val_key = validator_key or settings.validators[0].hotkey
        self._remote_nodes = {}
        self._remote_chutes = {}
        self._remote_images = {}
        self._remote_instances = {}
        self._remote_metrics = {}
        self._remote_utilizs = {}
        self._loop_interval = loop_interval
        self._count = 0

    async def synchronize(self):
        self._count += 1
        logger.debug(f"[{self._count}] Synchronizing remote objects ...")

        self._remote_nodes = await RemoteApi.get_nodes(self._val_key)
        self._remote_chutes = await RemoteApi.get_chutes(self._val_key)
        self._remote_images = await RemoteApi.get_images(self._val_key)
        self._remote_instances = await RemoteApi.get_instances(self._val_key)
        self._remote_metrics = await RemoteApi.get_metrics(self._val_key)
        self._remote_utilizs = await RemoteApi.get_utilization(self._val_key)

    async def run(self):

        logger.info("RemoteSync runing...")
        while True:
            try:
                await asyncio.sleep(self._loop_interval)
                await self.synchronize()
            except Exception as ex:
                logger.error(f"RemoteSync failed: {ex}\n{traceback.format_exc()}")

    # region properties

    @property
    def nodes(self) -> dict[str, dict]:
        return self._remote_nodes

    @property
    def chutes(self) -> dict[str, dict]:
        return self._remote_chutes

    @property
    def images(self) -> dict[str, dict]:
        return self._remote_images

    @property
    def instances(self) -> dict[str, dict]:
        return self._remote_instances

    @property
    def metrics(self) -> dict[str, dict]:
        return self._remote_metrics

    @property
    def utilization(self) -> dict[str, dict]:
        return self._remote_utilizs

    # endregion


class Autoscaler:

    def __init__(
        self,
        chute_deployer: ChuteDeployer,
        remote_synchro: RemoteSynchro,
        loop_interval: int = 5 * 60,
    ):
        self._chute_deployer = chute_deployer
        self._remote_synchro = remote_synchro
        self._loop_interval = loop_interval
        self._count = 0

    async def autoscale(self) -> None:

        self._count += 1
        logger.debug(f"[{self._count}] Determining to scale ...")

        chutes = self._remote_synchro.chutes
        metrics = self._remote_synchro.metrics
        utilizs = self._remote_synchro.utilization
        gains = await self._calc_chute_gains(chutes, metrics, utilizs)
        cands = await self._eval_chute_servers(gains)
        quals = [it for it in cands if it["chute_id"] in _CANDIDATE_CHUTES] or cands
        traces = self._chute_deployer.chute_traces
        traces = [{**it, **traces.get(it["chute_id"], {"failed": False})} for it in quals]
        unfailed = [it for it in traces if not it.get("failed")]

        choosed = None
        if len(unfailed) > 0:
            unfailed.sort(key=lambda it: it["value_ratio"], reverse=True)
            choosed = unfailed[0]
        elif len(traces) > 0:
            traces.sort(key=lambda it: (it.get("deleted_at"), it["value_ratio"] * -1), reverse=False)
            choosed = traces[0]
        elif len(cands) > 0:
            choosed = cands[0]

        if choosed:
            val_key = choosed["validator"]
            chute_id = choosed["chute_id"]
            server_id = choosed["server_id"]
            await self._chute_deployer.deploy(chute_id, server_id, val_key)

    async def run(self):

        logger.info("Autoscaler runing...")
        while True:
            try:
                await asyncio.sleep(self._loop_interval)
                await self.autoscale()
            except Exception as ex:
                logger.error(f"Autoscaling failed: {ex}\n{traceback.format_exc()}")

    # region static

    @staticmethod
    async def _calc_chute_gains(
        remote_chutes: dict[str, dict],
        remote_metrics: dict[str, dict],
        remote_utilizs: dict[str, dict],
    ) -> list[dict]:

        chute_gains = []

        for chute_id, chute_info in remote_chutes.items():

            val_key = settings.validators[0].hotkey
            chute_name = chute_info.get("name")
            chute_version = chute_info.get("version")
            chute_image = chute_info.get("image")
            is_private = not chute_info.get("preemptible", True)
            gpu_supp = sorted(chute_info.get("supported_gpus", []))
            gpu_count = chute_info.get("node_selector", {}).get("gpu_count", 0)

            if not chute_info.get("cords"):
                continue

            metric = remote_metrics.get(chute_id)
            if not metric:
                continue

            utiliz = remote_utilizs.get(chute_id)
            if not utiliz:
                continue
            if not utiliz.get("scalable") or utiliz.get("update_in_progress"):
                continue

            ins_count = metric.get("instance_count", 0)
            rate_limit = metric.get("rate_limit_count", 0)

            if ins_count >= 5:
                rate_limit /= 5

            cmp_time = metric.get("total_compute_time", 0)
            cmp_mltp = metric.get("compute_multiplier", 1)
            cmp_cons = cmp_time * cmp_mltp
            tot_invc = metric.get("total_invocations", 1)
            per_invc = cmp_cons / (tot_invc or 1.0)
            sum_cons = cmp_cons + per_invc * rate_limit
            ptt_gain = sum_cons / (ins_count + 1)

            chute_gains.append(
                {
                    "validator": val_key,
                    "chute_id": chute_id,
                    "chute_name": chute_name,
                    "chute_version": chute_version,
                    "chute_image": chute_image,
                    "is_private": is_private,
                    "potential_gain": ptt_gain,
                    "instance_count": ins_count,
                    "compute_multiplier": cmp_mltp,
                    "gpu_supported": gpu_supp,
                    "gpu_count": gpu_count,
                    "metrics": metric,
                    "utilization": utiliz,
                }
            )

        chute_gains = sorted(chute_gains, key=lambda x: x["potential_gain"], reverse=True)

        return chute_gains

    @staticmethod
    async def _query_gpu_servers(gpu_array: list[str], gpu_count: int) -> list[Server]:

        supported_gpus = list(gpu_array)

        # 若支持h200且有其他GPU，优先排除h200
        # if "h200" in supported_gpus and set(supported_gpus) - set(["h200"]):
        #     supported_gpus = list(set(supported_gpus) - set(["h200"]))

        # 子查询：统计每个服务器的总GPU数（支持的GPU且已验证）
        total_gpus_per_server = (
            select(Server.server_id, func.count(GPU.gpu_id).label("total_gpus"))
            .select_from(Server)
            .join(GPU, Server.server_id == GPU.server_id)
            .where(GPU.model_short_ref.in_(supported_gpus), GPU.verified.is_(True))
            .group_by(Server.server_id)
            .subquery()
        )
        # 子查询：统计每个服务器的已用GPU数（已验证且已分配部署）
        used_gpus_per_server = (
            select(Server.server_id, func.count(GPU.gpu_id).label("used_gpus"))
            .select_from(Server)
            .join(GPU, Server.server_id == GPU.server_id)
            .where(GPU.verified.is_(True), GPU.deployment_id.isnot(None))
            .group_by(Server.server_id)
            .subquery()
        )
        # 主查询：筛选可用服务器（空闲GPU满足需求、未锁定）
        query = (
            select(
                Server,
                total_gpus_per_server.c.total_gpus,
                func.coalesce(used_gpus_per_server.c.used_gpus, 0).label("used_gpus"),
                (
                    # 空闲GPU数 = 总GPU - 已用GPU
                    total_gpus_per_server.c.total_gpus
                    - func.coalesce(used_gpus_per_server.c.used_gpus, 0)
                ).label("free_gpus"),
            )
            .select_from(Server)
            .join(
                # 总 GPU
                total_gpus_per_server,
                Server.server_id == total_gpus_per_server.c.server_id,
            )
            .outerjoin(
                # 已用 GPU
                used_gpus_per_server,
                Server.server_id == used_gpus_per_server.c.server_id,
            )
            .join(GPU, Server.server_id == GPU.server_id)
            .where(
                GPU.model_short_ref.in_(supported_gpus),
                GPU.verified.is_(True),
                # 空闲GPU需满足chute的GPU数量需求
                (total_gpus_per_server.c.total_gpus - func.coalesce(used_gpus_per_server.c.used_gpus, 0) >= gpu_count),
                # 且服务器未锁定
                Server.locked.is_(False),
            )
            # 排序策略：优先选择每小时成本低、空闲GPU少的服务器
            .order_by(Server.hourly_cost.asc(), text("free_gpus ASC"))
        )

        async with get_session() as session:
            servers = (await session.execute(query)).unique().scalars().all()
            return list(servers)

    @staticmethod
    async def _eval_chute_servers(chute_gains: list[dict]) -> list[dict]:

        suit_servers = []
        for chute in chute_gains:

            gpu_array = chute["gpu_supported"]
            gpu_count = chute["gpu_count"]
            ptt_gain = chute["potential_gain"]
            cmp_mltp = chute["compute_multiplier"]
            prv_chute = chute["is_private"]

            servers = await Autoscaler._query_gpu_servers(gpu_array, gpu_count)
            for svr in servers:

                hour_cost = svr.hourly_cost
                val_ratio = 0
                # Adjust chute_value for private chutes,
                # which have a much different compute_units value of number of seconds * compute multiplier * 16.
                if prv_chute:
                    val_ratio = (60 * 60 * cmp_mltp * 16) / (hour_cost * gpu_count)
                else:
                    val_ratio = ptt_gain / (hour_cost * gpu_count)

                suit_servers.append(
                    {
                        **chute,
                        "value_ratio": val_ratio,
                        "hourly_cost": hour_cost,
                        "server_id": svr.server_id,
                        "server_ip": svr.ip_address,
                        "server_port": svr.verification_port,
                        "server_name": svr.name,
                        "server_gpus": svr.gpu_count,
                    }
                )

        suit_servers = sorted(suit_servers, key=lambda x: x["value_ratio"], reverse=True)
        return suit_servers

    # endregion


class Reconciler:

    MAX_INIT_AGE = 35 * 60
    MAX_STUB_AGE = 35 * 60

    def __init__(
        self,
        *,
        chute_deployer: ChuteDeployer,
        remote_synchro: RemoteSynchro,
        loop_interval: int = 15 * 60,
    ):

        self._chute_deployer = chute_deployer
        self._remote_synchro = remote_synchro
        self._loop_interval = loop_interval
        self._count = 0

    async def reconcile(self):

        async def _delete_depl(did, msg):
            try:
                logger.debug(f"Delete deployment ({did}): {msg}")
                await self._chute_deployer.undeploy(did)
            except Exception as ex:
                logger.error(f"Failed to delete deployment: {ex}\n{traceback.format_exc()}")

        async def _update_depl(did, val):
            try:
                logger.debug(f"Update deployment ({did}): {val}")
                await LocalManager.update_deployment(deployment_id=did, values=val)
            except Exception as ex:
                logger.error(f"Failed to update deployment: {ex}\n{traceback.format_exc()}")

        async def _delete_chute(cid, msg):
            try:
                logger.debug(f"Delete chute ({cid}): {msg}")
                await LocalManager.delete_chute(chute_id=cid)
            except Exception as ex:
                logger.error(f"Failed to delete chute: {ex}\n{traceback.format_exc()}")

        async def _update_chute(cid, val):
            try:
                logger.debug(f"Update chute ({cid}): {val}")
                await LocalManager.update_chute(chute_id=cid, values=val)
            except Exception as ex:
                logger.error(f"Failed to update chute: {ex}\n{traceback.format_exc()}")

        async def _create_chute(cid, chute):
            try:
                logger.debug(f"Create chute ({cid}): {chute.name=}")
                await LocalManager.create_chute(chute)
            except Exception as ex:
                logger.error(f"Failed to create chute: {ex}\n{traceback.format_exc()}")

        self._count += 1
        logger.debug(f"[{self._count}] Reconciling local objects ...")

        val_key = settings.validators[0].hotkey
        loc_depls = await LocalManager.get_deployments(val_key)
        rmt_insts = self._remote_synchro.instances
        chk_diffs = await self._check_deployments(val_key, loc_depls, rmt_insts)

        for did, msg in chk_diffs["deleted"].items():
            await _delete_depl(did, msg)

        for did, val in chk_diffs["updated"].items():
            await _update_depl(did, val)

        loc_chutes = await LocalManager.get_chutes(val_key)
        rmt_chutes = self._remote_synchro.chutes
        rmt_images = self._remote_synchro.images
        chk_diffs = await self._check_chutes(val_key, loc_chutes, rmt_chutes, rmt_images)

        for cid, msg in chk_diffs["deleted"].items():
            await _delete_chute(cid, msg)

        for cid, val in chk_diffs["updated"].items():
            await _update_chute(cid, val)

        for cid, chute in chk_diffs["created"].items():
            await _create_chute(cid, chute)

        # await asyncio.gather(*tasks)

    async def run(self):

        logger.info("Reconciler runing ...")
        while True:
            try:
                await asyncio.sleep(self._loop_interval)
                await self.reconcile()
            except Exception as ex:
                logger.error(f"Reconciling failed: {ex}\n{traceback.format_exc()}")

    # region static

    @staticmethod
    async def _check_chutes(
        val_key: str, local_chutes: list[Chute], remote_chutes: dict[str, dict], remote_images: dict[str, dict]
    ) -> dict:

        should_created = {}
        should_deleted = {}
        should_updated = {}

        image_dict = {}
        for iid, item in remote_images.items():
            img_str = f"{item['username']}/{item['name']}:{item['tag']}"
            if item.get("patch_version") and item["patch_version"] != "initial":
                img_str += f"-{item['patch_version']}"
            image_dict[iid] = {
                "image": img_str,
                "chutes_version": item["chutes_version"],
            }

        remote_hashes = {}
        for cid, item in remote_chutes.items():

            iid = item.get("image_id")
            img = image_dict.get(iid)
            if img and (item["image"] != img["image"] or item["chutes_version"] != img["chutes_version"]):
                item["image"] = img["image"]
                item["chutes_version"] = img["chutes_version"]

                logger.warning(f"Chute ({cid}) image updated: {img['image']}, chutes_version: {img['chutes_version']}")

            hash = hashlib.sha256(
                "\n".join(
                    [
                        item["name"],
                        item["image"],
                        item["code"],
                        item["ref_str"],
                        item["filename"],
                        item["version"],
                        f"{item['chutes_version']}",
                        f"{item['node_selector']['gpu_count']}",
                        f"{set(sorted(item['supported_gpus']))}",
                        f"{item['preemptible']}",
                    ]
                ).encode()
            ).hexdigest()
            remote_hashes[cid] = hash

        local_cids = set()
        for chute in local_chutes:
            cid = chute.chute_id
            local_cids.add(cid)
            hash = hashlib.sha256(
                "\n".join(
                    [
                        chute.name,
                        chute.image,
                        chute.code,
                        chute.ref_str,
                        chute.filename,
                        chute.version,
                        f"{chute.chutes_version}",
                        f"{chute.gpu_count}",
                        f"{set(sorted(chute.supported_gpus))}",
                        f"{chute.preemptible}",
                    ]
                ).encode()
            ).hexdigest()

            if cid in remote_hashes:
                if hash == remote_hashes[cid]:
                    continue

                item = remote_chutes[cid]
                vals = {}
                for key in (
                    "name",
                    "image",
                    "code",
                    "ref_str",
                    "filename",
                    "version",
                    "chutes_version",
                    "preemptible",
                ):
                    vals[key] = item.get(key)
                vals["supported_gpus"] = sorted(item["supported_gpus"])
                vals["gpu_count"] = item["node_selector"]["gpu_count"]
                vals["ban_reason"] = None
                should_updated[cid] = vals
            else:
                should_deleted[cid] = f"Not found chute in remote chutes by chute_id={cid}"

        for cid, item in remote_chutes.items():
            if cid not in local_cids:
                should_created[cid] = Chute(
                    validator=val_key,
                    chute_id=cid,
                    name=item["name"],
                    image=item["image"],
                    code=item["code"],
                    filename=item["filename"],
                    ref_str=item["ref_str"],
                    version=item["version"],
                    supported_gpus=sorted(item["supported_gpus"]),
                    gpu_count=item["node_selector"]["gpu_count"],
                    chutes_version=item["chutes_version"],
                    preemptible=item["preemptible"],
                    ban_reason=None,
                )

        return {
            "created": should_created,
            "deleted": should_deleted,
            "updated": should_updated,
        }

    @staticmethod
    async def _check_deployments(
        val_key: str, local_depls: list[Deployment], remote_instances: dict[str, dict]
    ) -> dict:

        should_deleted = {}
        should_updated = {}

        # Build map of config_id -> instance from remote inventory.
        rmt_configs = {}
        for iid, item in remote_instances.items():
            cfg_id = item.get("config_id")
            if cfg_id:
                rmt_configs[cfg_id] = {"instance_id": iid}

        # Get all pods with config_id labels for orphan detection
        k8s_cfg_ids = set()
        try:
            pods = await ClusterManager.get_pods(label_selector="chutes/config-id")
            k8s_cfg_ids = {p["labels"]["chutes/config-id"] for p in pods}
        except Exception as ex:
            raise RuntimeError(f"Failed to get pods by config-id label: {ex}")

        # Get deployment IDs of job-based chutes
        k8s_dep_ids = set()
        try:
            chts = await ClusterManager.get_deployed_chutes()
            k8s_dep_ids = {c["deployment_id"] for c in chts}
        except Exception as ex:
            raise RuntimeError(f"Failed to get deployed chutes: {ex}")

        for depl in local_depls:

            depl_id = depl.deployment_id
            updates = {}

            # Make sure the instances created with launch configs have the instance ID tracked.
            if depl.config_id and not depl.instance_id:
                conf = rmt_configs.get(depl.config_id)
                if conf:
                    inst_id = conf["instance_id"]
                    depl.instance_id = inst_id
                    updates["instance_id"] = inst_id
                else:
                    should_deleted[depl_id] = f"Not found deployment in remote instances by {depl.config_id=}"
                    continue

            # Early check for orphaned deployments with config_id
            if depl.config_id and depl.config_id not in k8s_cfg_ids:
                should_deleted[depl_id] = f"Not found deployment in K8s pods by {depl.config_id=}"
                continue

            # Check if instance exists on validator
            if depl.instance_id:
                inst = remote_instances.get(depl.instance_id)
                if inst and inst.get("version") == depl.version:
                    # Reconcile the verified/active state for instances.
                    if inst.get("last_verified_at") and not depl.verified_at:
                        verf_at = func.now()
                        depl.verified_at = verf_at
                        updates["verified_at"] = verf_at
                    if inst.get("active") and not depl.active:
                        act_at = func.now()
                        depl.active = True
                        depl.activated_at = act_at
                        depl.stub = False
                        updates["active"] = True
                        updates["activated_at"] = act_at
                        updates["stub"] = False
                else:
                    should_deleted[depl_id] = (
                        f"Not found deployment in remote instances by {depl.instance_id=} & {depl.version=}"
                    )
                    continue

            # Special handling for deployments with job_id
            if hasattr(depl, "job_id") and depl.job_id:
                # Always track the instance_id for job deployments to prevent premature purging
                pass

            # Normal deployment handling (no job_id)

            # Check if deployment exists in k8s
            # Delete deployments that never made it past stub stage or disappeared from k8s
            if depl_id not in k8s_dep_ids and not depl.stub:
                should_deleted[depl_id] = f"Not found deployment in K8s cluster by {depl.deployment_id=}"
                continue

            depl_age = datetime.now(timezone.utc) - depl.created_at

            # Clean up old stubs
            if (depl.stub or not depl.instance_id) and depl_age > timedelta(seconds=Reconciler.MAX_STUB_AGE):
                should_deleted[depl_id] = f"Deployment timed out with stub or no instance ({depl.deployment_id=})"
                continue

            # Check for terminated jobs or jobs that never started
            if (
                (not depl.active)
                or (depl.verified_at is None)
                and depl_age > timedelta(seconds=Reconciler.MAX_INIT_AGE)
            ):
                try:
                    kdep = await ClusterManager.get_deployment(depl_id)
                except Exception as ex:
                    exmsg = f"Failed to get k8s deployment ({depl_id}): {ex}"
                    should_deleted[depl_id] = exmsg
                    logger.error(exmsg)
                    continue

                # Check job completion status
                job_status = kdep.get("status", {})
                if job_status.get("succeeded", 0) > 0:
                    should_deleted[depl_id] = f"Deployment status succeeded ({depl.deployment_id=})"
                elif job_status.get("failed", 0) > 0:
                    should_deleted[depl_id] = f"Deployment status failed ({depl.deployment_id=})"
                else:
                    continue

                # Check for terminated pods (for Jobs that don't update status properly)
                for pod in kdep.get("pods", []):
                    pod_state = pod.get("state", {})
                    if pod_state.get("terminated"):
                        exit_code = pod_state.get("terminated").get("exit_code", 0)
                        should_deleted[depl_id] = f"Deployment pod terminated with {exit_code=} ({depl.deployment_id=})"
                        continue

            if updates:
                should_updated[depl_id] = updates

        # Purge validator instances not deployed locally
        # if instance_id not in all_instances and (not config_id or config_id not in all_configs)
        # purge_validator_instance(vali, chute_id, instance_id)

        # Purge k8s deployments that aren't tracked anymore
        # for deployment_id in all_k8s_ids - all_deployments:
        # undeploy(deployment_id)

        return {
            "deleted": should_deleted,
            "updated": {k: v for k, v in should_updated.items() if k not in should_deleted},
        }

    @staticmethod
    async def _check_gpus() -> dict:
        pass

    @staticmethod
    async def _check_servers() -> dict:
        pass

    @staticmethod
    async def check_k8s_services() -> dict:
        pass


# endregion


class CmdHandler:

    @staticmethod
    def list_nodes():

        async def _load_list():
            nodes = await ClusterManager.get_nodes()
            nodes.sort(key=lambda it: it["name"])
            return nodes

        def _show_list(items: list[dict]):
            dash_line = "-" * 110
            none_line = "[ --------- NONE --------- ]"
            print(dash_line)
            print(f"{'server_id':<37} {'node_name':<25} {'ip_address':<16} {'status':<8}  {'disk total | used'}")
            print(dash_line)

            if not items:
                print(none_line)

            for it in items:
                print(
                    f"{it['server_id']:<37} "
                    f"{it['name']:<25} "
                    f"{it['ip_address']:<16} "
                    f"{(it['status'] or '/'):<8}  "
                    f"{it['disk_total_gb']:<10.1f} | {it['disk_used_gb']:<0.1f} GB"
                )
            print(dash_line)

        items = asyncio.run(_load_list())
        _show_list(items)
        logger.info(f"Listed cluster nodes: {len(items)}")

    @staticmethod
    def list_pods(namespace: str = None) -> None:

        async def _load_list(ns):
            pods = await ClusterManager.get_pods(namespace)
            pods.sort(key=lambda it: (it["namespace"], it["node_name"], it["name"]))
            return pods

        def _show_list(items: list[dict]):
            dash_line = "-" * 150
            none_line = "[ --------- NONE --------- ]"
            print(dash_line)
            print(f"{'namespace':<15} {'node_name':<25} {'pod_name':<50} {'status':<12} {'pod_ip':<16} {'start_time'}")
            print(dash_line)

            if not items:
                print(none_line)

            for it in items:
                print(
                    f"{it['namespace']:<15} "
                    f"{it['node_name']:<25} "
                    f"{it['name']:<50} "
                    f"{(it['status'] or '/'):<12} "
                    f"{it['pod_ip']:<16} "
                    f"{it['start_time']}"
                )

            print(dash_line)

        items = asyncio.run(_load_list(namespace))
        _show_list(items)
        logger.info(f"Listed cluster pods: {len(items)}")

    @staticmethod
    def list_instances(node_name: str = None) -> None:

        async def _load_list(node: str) -> list[dict]:
            insts = await ClusterManager.get_deployed_chutes()
            if node:
                insts = [it for it in insts if node in it["node_name"]]

            for inst in insts:
                chute = await LocalManager.get_chute(inst["chute_id"])
                if chute:
                    inst["chute_name"] = chute.name
                    inst["gpu_count"] = chute.gpu_count
                else:
                    inst["chute_name"] = "[NONE]"
                    inst["gpu_count"] = 0

            insts.sort(key=lambda x: (-x["ready"], x["node_name"], x["chute_name"]))
            return insts

        def _show_list(items: list[dict]):
            dash_line = "-" * 175
            none_line = "[ --------- NONE --------- ]"
            print(dash_line)
            print(
                f"{'deployment_id':<37} {'node_name':<22} {'chute_name':<50} {'gpu'} {'ready':<5}  {'status':<9} {'pod_state':<9}  {'start_time':<11} {'reason / message'}"
            )
            print(dash_line)

            if not items:
                print(none_line)

            for it in items:
                print(
                    f"{it['deployment_id']:<37} "
                    f"{it['node_name']:<22} "
                    f"{it['chute_name'][:50]:<50} "
                    f"{it['gpu_count']:<3} "
                    f"{str(it['ready']):<5}  "
                    f"{(it['status'] or '/'):<9} "
                    f"{(it['pod_phase'] or '/'):<9}  "
                    f"{it['start_time'].strftime('%m-%d %H:%M'):<11} "
                    f"{it['reason'] or ''} {it['message'] or ''} "
                )
            print(dash_line)

        items = asyncio.run(_load_list(node_name))
        _show_list(items)
        logger.info(f"Listed deployed instances: {len(items)}")

    @staticmethod
    def list_chute_gains(gpu_code: str | None = None) -> None:

        async def _load_list(gpu):
            val_key = settings.validators[0].hotkey
            rmt_chutes = await RemoteApi.get_chutes(val_key)
            rmt_metrics = await RemoteApi.get_metrics(val_key)
            rmt_utilizs = await RemoteApi.get_utilization(val_key)
            cht_gains = await Autoscaler._calc_chute_gains(rmt_chutes, rmt_metrics, rmt_utilizs)

            if gpu:
                cht_gains = [it for it in cht_gains if gpu in it["gpu_supported"]]

            return cht_gains

        def _show_list(items: list[dict]):
            dash_line = "-" * 160
            none_line = "[ --------- NONE --------- ]"
            print(dash_line)
            print(
                f"{'chute_id':<37} {'chute_name':<35} {'gain':>10} {'inst':>5}  {'gpu_supported':<10} | {'gpu_count'}"
            )
            print(dash_line)

            if not items:
                print(none_line)

            for it in items:
                print(
                    f"{it['chute_id']:<37} "
                    f"{it['chute_name'][:35]:<35} "
                    f"{it['potential_gain']:>10.2f} "
                    f"{it['instance_count']:>5}  "
                    f"{','.join(it['gpu_supported'])}  | "
                    f"{it['gpu_count']}"
                )
            print(dash_line)

        items = asyncio.run(_load_list(gpu_code))
        _show_list(items)
        logger.info(f"Calculated chute gains: {len(items)}")

    @staticmethod
    def list_chute_servers(gpu_code: str | None = None, server_name: str | None = None) -> None:

        async def _load_list(gpu, svr):
            val_key = settings.validators[0].hotkey
            rmt_chutes = await RemoteApi.get_chutes(val_key)
            rmt_metrics = await RemoteApi.get_metrics(val_key)
            rmt_utilizs = await RemoteApi.get_utilization(val_key)
            gains = await Autoscaler._calc_chute_gains(rmt_chutes, rmt_metrics, rmt_utilizs)
            if gpu_code:
                gains = [it for it in gains if gpu in it["gpu_supported"]]
            servers = await Autoscaler._eval_chute_servers(gains)
            if svr:
                servers = [it for it in servers if svr in it.get("server_name")]
            return servers

        def _show_list(items: list[dict]):
            dash_line = "-" * 160
            none_line = "[ --------- NONE --------- ]"
            print(dash_line)
            print(
                f"{'chute_id':<37} {'chute_name':<35}  {'server_id':<37} {'server_name':<22} {'value':>10} {'cost':>6}  {'gpu'}"
            )
            print(dash_line)

            if not items:
                print(none_line)

            for it in items:
                print(
                    f"{it['chute_id']:<37} "
                    f"{it['chute_name'][:35]:<35}  "
                    f"{it['server_id']:<37} "
                    f"{it['server_name'][:22]:<22} "
                    f"{it['value_ratio']:>10.2f}  "
                    f"{it['hourly_cost']:>5.2f}   "
                    f"{it['gpu_count']}"
                )
            print(dash_line)

        items = asyncio.run(_load_list(gpu_code, server_name))
        _show_list(items)
        logger.info(f"Evaluated chute servers: {len(items)}")

    @staticmethod
    def recycle(force: bool = False):

        async def _recyle_action():
            depl = ChuteDeployer()
            sync = RemoteSynchro()
            recl = Reconciler(
                chute_deployer=depl,
                remote_synchro=sync,
            )
            await sync.synchronize()
            await recl.reconcile()

        asyncio.run(_recyle_action())

    @staticmethod
    def deploy(chute: str, server: str, auto: bool = False):

        async def _deploy_action(cht, svr):
            depl = ChuteDeployer()
            await depl.deploy(cht, svr)

        asyncio.run(_deploy_action(chute, server))

    @staticmethod
    def undeploy(depl_id: str):

        async def _undeploy_action(did):
            depl = ChuteDeployer()
            await depl.undeploy(did)

        asyncio.run(_undeploy_action(depl_id))


class Main:

    @staticmethod
    def default():
        gepetto = Gepetto()
        asyncio.run(gepetto.run())
        # Main.dry_run()

    @staticmethod
    def dry_run():
        async def _empty():
            while True:
                print("Main dry running --->>> ")
                await asyncio.sleep(600)

        asyncio.run(_empty())

    @staticmethod
    def init_cmds() -> Dict[str, Callable]:

        return {
            "nodes": CmdHandler.list_nodes,
            "pods": CmdHandler.list_pods,
            "insts": CmdHandler.list_instances,
            "chutes": CmdHandler.list_chute_gains,
            "scales": CmdHandler.list_chute_servers,
            "recycle": CmdHandler.recycle,
            "deploy": CmdHandler.deploy,
            "undeploy": CmdHandler.undeploy,
        }

    @staticmethod
    def parse_args():

        parser = argparse.ArgumentParser(description="Chutes Miner Commands")

        subprs = parser.add_subparsers(dest="cmd", help="Available commands")

        nd = subprs.add_parser("nodes", help="List K8s nodes in the cluster")

        pd = subprs.add_parser("pods", help="List K8s pods in the cluster")
        pd.add_argument("-n", "--ns", type=str, help="Namespace")

        it = subprs.add_parser("insts", help="List running instances for chutes")
        it.add_argument("-d", "--node", type=str, help="Node name")

        ch = subprs.add_parser("chutes", help="List remote available chutes")
        ch.add_argument("-g", "--gpu", type=str, help="GPU Code, like: h200 or a100 or 5090")
        # ch.add_argument("-a", "--all", action="store_true", help="Show all chutes")

        sc = subprs.add_parser("scales", help="List scalable chutes & servers")
        sc.add_argument("-g", "--gpu", type=str, help="GPU Code, like: h200 or a100 or 5090")
        sc.add_argument("-s", "--server", type=str, help="Server name, like: chutes-miner-gpu-01")

        re = subprs.add_parser("recycle", help="Recycle invalid deployments")
        # re.add_argument("-f", "--force", action="store_true", help="Force clean")

        de = subprs.add_parser("deploy", help="Deploy chute on server")
        de.add_argument("-c", "--chute", type=str, help="Chute ID or name")
        de.add_argument("-s", "--server", type=str, help="Server ID or name")
        # de.add_argument("-a", "--auto", action="store_true", help="Auto deploy")

        un = subprs.add_parser("undeploy", help="Undeploy chute from server")
        un.add_argument("-i", "--depl_id", type=str, help="Deployment ID")
        # un.add_argument("-a", "--all", action="store_true", help="Undeploy all instances")

        args = parser.parse_args()
        args.print_help = parser.print_help

        return args

    @staticmethod
    def run():
        try:
            cmds = Main.init_cmds()
            args = Main.parse_args()

            if hasattr(args, "cmd") and args.cmd in cmds:
                func = cmds.get(args.cmd)
                keys = inspect.signature(func).parameters.keys()
                pars = {k: v for k, v in args.__dict__.items() if k in keys}
                func(**pars)
            else:
                Main.default()

        except Exception as ex:
            logger.error(f"{ex}\n{traceback.format_exc()}")


if __name__ == "__main__":
    Main.run()
