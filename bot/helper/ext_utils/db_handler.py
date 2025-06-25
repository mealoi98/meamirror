import json
import asyncio
from pathlib import Path

from ... import LOGGER, user_data, rss_dict, qbit_options
from ...core.mltb_client import TgClient
from ...core.config_manager import Config

DATA_FILE = Path("data.json")


class DbManager:
    def __init__(self):
        self._return = False
        self._lock = asyncio.Lock()
        self._ensure_file()

    def _ensure_file(self):
        if not DATA_FILE.exists():
            with open(DATA_FILE, "w") as f:
                json.dump({}, f)

    async def _read(self):
        async with self._lock:
            with open(DATA_FILE, "r") as f:
                return json.load(f)

    async def _write(self, data):
        async with self._lock:
            with open(DATA_FILE, "w") as f:
                json.dump(data, f, indent=2)

    async def update_deploy_config(self):
        if self._return:
            return
        settings = import_module("config")
        config_file = {
            key: value.strip() if isinstance(value, str) else value
            for key, value in vars(settings).items()
            if not key.startswith("__")
        }
        data = await self._read()
        data.setdefault("config", {}).update(config_file)
        await self._write(data)

    async def update_config(self, dict_):
        if self._return:
            return
        data = await self._read()
        data.setdefault("config", {}).update(dict_)
        await self._write(data)

    async def update_aria2(self, key, value):
        if self._return:
            return
        data = await self._read()
        data.setdefault("aria2c", {})[key] = value
        await self._write(data)

    async def update_qbittorrent(self, key, value):
        if self._return:
            return
        data = await self._read()
        data.setdefault("qbittorrent", {})[key] = value
        await self._write(data)

    async def save_qbit_settings(self):
        if self._return:
            return
        data = await self._read()
        data["qbit_options"] = qbit_options
        await self._write(data)

    async def update_private_file(self, path):
        if self._return:
            return
        db_path = path.replace(".", "__")
        if await aiopath.exists(path):
            async with aiopen(path, "rb+") as pf:
                pf_bin = await pf.read()
            data = await self._read()
            files = data.setdefault("files", {})
            files[db_path] = pf_bin.decode(errors="ignore")
            await self._write(data)
            if path == "config.py":
                await self.update_deploy_config()
        else:
            data = await self._read()
            files = data.setdefault("files", {})
            files.pop(db_path, None)
            await self._write(data)

    async def update_nzb_config(self):
        if self._return:
            return
        async with aiopen("sabnzbd/SABnzbd.ini", "rb+") as pf:
            nzb_conf = await pf.read()
        data = await self._read()
        data.setdefault("nzb", {})["SABnzbd__ini"] = nzb_conf.decode(errors="ignore")
        await self._write(data)

    async def update_user_data(self, user_id):
        if self._return:
            return
        data = user_data.get(user_id, {})
        data = data.copy()
        for key in ("THUMBNAIL", "RCLONE_CONFIG", "TOKEN_PICKLE"):
            data.pop(key, None)
        db_data = await self._read()
        users = db_data.setdefault("users", {})
        users[str(user_id)] = data
        await self._write(db_data)

    async def update_user_doc(self, user_id, key, path=""):
        if self._return:
            return
        db_data = await self._read()
        users = db_data.setdefault("users", {})
        if path:
            async with aiopen(path, "rb+") as doc:
                doc_bin = await doc.read()
            users[str(user_id)][key] = doc_bin.decode(errors="ignore")
        else:
            users[str(user_id)].pop(key, None)
        await self._write(db_data)

    async def rss_update_all(self):
        if self._return:
            return
        data = await self._read()
        rss = data.setdefault("rss", {})
        for user_id in list(rss_dict.keys()):
            rss[str(user_id)] = rss_dict[user_id]
        await self._write(data)

    async def rss_update(self, user_id):
        if self._return:
            return
        data = await self._read()
        rss = data.setdefault("rss", {})
        rss[str(user_id)] = rss_dict[user_id]
        await self._write(data)

    async def rss_delete(self, user_id):
        if self._return:
            return
        data = await self._read()
        rss = data.setdefault("rss", {})
        rss.pop(str(user_id), None)
        await self._write(data)

    async def add_incomplete_task(self, cid, link, tag):
        if self._return:
            return
        data = await self._read()
        tasks = data.setdefault("tasks", [])
        tasks.append({"cid": cid, "link": link, "tag": tag})
        await self._write(data)

    async def rm_complete_task(self, link):
        if self._return:
            return
        data = await self._read()
        tasks = data.get("tasks", [])
        data["tasks"] = [t for t in tasks if t["link"] != link]
        await self._write(data)

    async def get_incomplete_tasks(self):
        notifier_dict = {}
        if self._return:
            return notifier_dict
        data = await self._read()
        tasks = data.get("tasks", [])
        for row in tasks:
            if row["cid"] in list(notifier_dict.keys()):
                if row["tag"] in list(notifier_dict[row["cid"]]):
                    notifier_dict[row["cid"]][row["tag"]].append(row["link"])
                else:
                    notifier_dict[row["cid"]][row["tag"]] = [row["link"]]
            else:
                notifier_dict[row["cid"]] = {row["tag"]: [row["link"]]}
        await self.db.tasks[TgClient.ID].drop()
        return notifier_dict

    async def trunc_table(self, name):
        if self._return:
            return
        data = await self._read()
        data.pop(name, None)
        await self._write(data)

    # Dummy connect/disconnect for compatibility
    async def connect(self):
        pass

    async def disconnect(self):
        pass


database = DbManager()
