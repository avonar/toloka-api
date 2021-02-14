import asyncio
import time
from json import JSONDecodeError
from logging import Logger
from toloka_api.constants import API_V1, TOO_MANY_REQUESTS, TIMEOUT, AssigmentStatus
from aiohttp import ClientSession
from typing import Any, List, Union

import aiohttp
from aiohttp import ContentTypeError

log = Logger('Toloka api')


def retry(func):
    async def wrapper(*args, **kwargs):
        result = None
        for i in range(15):
            try:
                result = await func(*args, **kwargs)
                if not result or result.get('code') == TOO_MANY_REQUESTS:
                    time.sleep(i ^ 2 + 1)
                    continue
                break
            except asyncio.TimeoutError:
                log.error('timeout')
        if result is None or result.get('code') == TOO_MANY_REQUESTS:
            log.error(f'Some malfunctions with Toloka requests. \nresult: \n{result}')
            exit(1)

        return result

    return wrapper


class AsyncRest:
    def __init__(self, host: str, session: ClientSession, headers: dict = None, proxies: dict = None):
        self.host = host
        self.headers = headers
        self.proxies = proxies
        self.session = session

    @retry
    async def _send_request(self, method: str, path: str, **kwargs) -> Any:
        url = f'{self.host}{path}'
        log.debug(f'{method} {url}')
        log.info(f'_send_request({method}, {url})')
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)  # type: ignore
        try:
            async with self.session.request(method=method,
                                            url=url,
                                            headers=kwargs.pop('headers', self.headers),
                                            timeout=kwargs.pop('timeout', timeout),
                                            **kwargs) as response:
                try:
                    res = await response.json()
                    return res
                except (ContentTypeError, Exception):
                    log.error(f'Response error {method}, {url}, {await response.text()}', exc_info=True)
        except Exception:
            log.error(f'Request error', exc_info=True)

    async def post(self, path: str, json=None, **kwargs) -> Any:
        resp = await self._send_request('POST', path, json=json, **kwargs)
        return resp

    async def get(self, path: str, params=None, **kwargs) -> Any:
        resp = await self._send_request('GET', path, params=params, **kwargs)
        return resp

    async def patch(self, path: str, json=None, **kwargs) -> Any:
        resp = await self._send_request('PATCH', path, json=json, **kwargs)
        return resp

    async def put(self, path: str, json=None, **kwargs) -> Any:
        resp = await self._send_request('PUT', path, json=json, **kwargs)
        return resp

    def __del__(self):
        if not self.session.closed:
            if self.session._connector_owner:
                self.session._connector.close()
            self.session._connector = None


class TolokaClient(object):
    def __init__(self, oauth_token: str, sandbox: bool = False):
        session = ClientSession()
        if sandbox:
            self.api = AsyncRest(f'https://sandbox.toloka.yandex.ru', session)
        else:
            self.api = AsyncRest(f'https://toloka.yandex.ru', session)
        self.headers = {'Authorization': f'OAuth {oauth_token}'}
        self.headers.update({'Content-Type': 'application/json'})

    async def get_projects(self, params=None) -> Any:
        res = await self.api.get(
            path=API_V1.ASSIGMENTS,
            headers=self.headers,
            params=params,
        )
        return res

    async def get_project(self, project_id: int, params=None) -> Any:
        res = await self.api.get(
            path=f'{API_V1.PROJECT}/{project_id}',
            headers=self.headers,
            params=params,
        )
        return res

    async def get_workers_count(self, params=None) -> Any:
        res = await self.api.get(
            path=API_V1.WORKERS,
            headers=self.headers,
            params=params,
        )
        return res['totalElements']

    async def get_workers(self, params=None) -> Any:
        if params is None:
            params = {}
        workers = []
        params['size'] = 100
        first_page_json_ = await self.api.get(
            path=API_V1.WORKERS,
            headers=self.headers,
            params=params,
        )
        first_page_json = first_page_json_
        workers += first_page_json['content']

        for page_index in range(1, first_page_json['totalPages']):
            params['page'] = page_index
            page_json_ = await self.api.get(
                path=API_V1.WORKERS,
                headers=self.headers,
                params=params,
            )
            page_json = page_json_
            workers += page_json['content']

        return workers

    async def get_pools_list(self, params) -> Any:
        """
        Get pools by parameters.
            Example: {'status': 'OPEN', 'project_id': project_id}
        """
        res = await self.api.get(
            path=API_V1.POOLS,
            headers=self.headers,
            params=params,
        )
        return res

    async def archive_pool(self, pool_id: int) -> Any:
        """
        Archive pool by id
        """
        res = await self.api.post(
            path=f'{API_V1.POOLS}/{pool_id}/archive',
            headers=self.headers,
        )
        return res

    async def get_all_pools(self, limit=300, **kwargs) -> Any:
        """
        Get all pools 
        Example: get_all_pools(project_id=project_id, created_gte=date)
        """
        pools = []
        pool_params = {'sort': 'id', 'limit': limit, **kwargs}
        pools_ = await self.get_pools_list(pool_params)
        pools.extend(pools_['items'])
        while pools_['has_more']:
            pool_params = {**pool_params, 'id_gt': pools_['items'][-1]['id']}
            pools_ = await self.get_pools_list(pool_params)
            pools.extend(pools_['items'])
        return pools

    async def get_pool(self, pool_id: int) -> Any:
        """
        Get pool by id
        """
        res = await self.api.get(
            path=f'{API_V1.POOLS}/{pool_id}',
            headers=self.headers,
        )
        return res

    async def create_pool(self, params) -> Any:
        """
        Create new POOL.
        """
        res = await self.api.post(path=API_V1.POOLS, headers=self.headers, json=params)
        return res

    async def __open_close_pool(self, pool_id: int, operation_type) -> Any:
        """
        Open/ close pool
        """
        res = await self.api.post(path=f'{API_V1.POOLS}/{pool_id}/{operation_type}', headers=self.headers)
        return res

    async def start_pool(self, pool_id: int) -> dict:
        res = await self.__open_close_pool(pool_id, 'open')
        return res

    async def stop_pool(self, pool_id: int) -> dict:
        res = await self.__open_close_pool(pool_id, 'close')
        return res

    async def patch_pool(self, pool_id: int, pool_json: dict) -> Any:
        """
        Patch pool with new json
        """
        res = await self.api.put(path=f'{API_V1.POOLS}/{pool_id}', headers=self.headers, json=pool_json)
        return res

    async def clone_pool(self, pool_id: int) -> Any:
        res = await self.api.post(
            path=f'{API_V1.POOLS}/{pool_id}/clone',
            headers=self.headers,
        )
        return res

    async def get_task_list(self, params: dict) -> Any:
        res = await self.api.get(
            path=API_V1.TASKS,
            headers=self.headers,
            params=params,
        )
        return res

    async def get_task(self, task_id) -> Any:
        """
        Return task by task id
        """
        res = await self.api.get(
            path=f'{API_V1.TASKS}/{task_id}',
            headers=self.headers,
        )
        return res

    async def get_all_tasks(self, pool_id: int, limit=1000, **kwargs) -> Any:
        """
        Return all tasks from pool
        """
        tasks = []
        task_params = {'pool_id': pool_id, 'sort': 'id', 'limit': limit, **kwargs}
        task_suites = await self.get_task_list(task_params)
        tasks.extend(task_suites['items'])
        while task_suites['has_more']:
            task_params = {**task_params, 'id_gt': task_suites['items'][-1]['id']}
            task_suites = await self.get_task_list(task_params)
            tasks.extend(task_suites['items'])
        return tasks

    async def create_task(self, json, params=None, **kwargs) -> Any:
        res = await self.api.post(**kwargs, path=f'{API_V1.TASKS}', headers=self.headers, json=json, params=params)
        try:
            return res
        except JSONDecodeError:
            log.error('Fail while creating task', exc_info=True)

    async def patch_task_overlap(self, task, overlap) -> Any:
        """
        Change task overlap
        """
        res = await self.api.patch(
            path=f'{API_V1.TASKS}/{task}',
            headers=self.headers,
            json={'overlap': overlap},
        )
        return res

    async def get_task_suites_list(self, params) -> Any:
        res = await self.api.get(
            path=API_V1.TASK_SUITES,
            headers=self.headers,
            params=params,
        )
        return res

    async def get_task_suite(self, task_suite_id):
        """Return task suite by suite id"""
        res = await self.api.get(
            path=f'{API_V1.TASK_SUITES}/{task_suite_id}',
            headers=self.headers,
        )
        return res

    async def get_all_task_suites(self, pool_id) -> Any:
        suites = []
        limit = 100
        task_params = {'pool_id': pool_id, 'sort': 'id', 'limit': limit}
        task_suites = await self.get_task_suites_list(task_params)
        suites.extend(task_suites['items'])
        while task_suites['has_more']:
            task_params = {'pool_id': pool_id, 'sort': 'id', 'limit': limit, 'id_gt': task_suites['items'][-1]['id']}
            task_suites = await self.get_task_suites_list(task_params)
            suites.extend(task_suites['items'])
        return suites

    async def patch_task_suites_overlap(self, suit, overlap) -> Any:
        res = await self.api.patch(
            path=f'{API_V1.TASK_SUITES}/{suit}',
            headers=self.headers,
            json={'overlap': overlap},
        )
        return res

    async def get_assigments(self, params, **kwargs) -> dict:
        res = await self.api.get(
            **kwargs,
            path=API_V1.ASSIGMENTS,
            headers=self.headers,
            params=params,
        )
        return res

    async def get_all_assigments(self, pool_id, limit=1000, params={}, **kwargs) -> list:
        """
        Return all asigments.
        :params: additional params
        """
        results = []
        task_params = {'sort': 'id', 'limit': limit, 'pool_id': pool_id, **params}
        task_results = await self.get_assigments(task_params, **kwargs)
        results.extend(task_results.get('items'))  # type: ignore
        log.debug(len(results))
        while task_results['has_more']:
            task_params = {**task_params, 'id_gt': task_results['items'][-1]['id']}
            task_results = await self.get_assigments(task_params, **kwargs)
            results.extend(task_results['items'])
            log.debug(len(results))
        return results

    async def get_assigment_info(self, task_id) -> Any:
        res = await self.api.get(
            path=f'{API_V1.ASSIGMENTS}/{task_id}',
            headers=self.headers,
        )
        return res

    async def _proceed_assigment(self, res_id, params) -> Any:
        """
        Accept or reject assigment
        Example:
            params =  {'status': '<статус ответа>', 'public_comment': '<комментарий>'}
        """
        res = await self.api.patch(path=f'{API_V1.ASSIGMENTS}/{res_id}', headers=self.headers, json=params, timeout=10)
        return res

    async def accept_assigment(self, res_id: str, public_comment: str) -> Any:
        """
        Accept assigment
        """
        params = {'status': AssigmentStatus.ACCEPTED}
        if public_comment:
            params.update({'public_comment': public_comment})

        res = await self.api.patch(path=f'{API_V1.ASSIGMENTS}/{res_id}', headers=self.headers, json=params, timeout=10)
        return res

    async def reject_assigment(self, res_id: str, public_comment: str = 'Bad.') -> Any:
        """
        Accept assigment
        """
        params = {'status': AssigmentStatus.REJECTED, 'public_comment': public_comment}

        res = await self.api.patch(path=f'{API_V1.ASSIGMENTS}/{res_id}', headers=self.headers, json=params, timeout=10)
        return res

    async def send_message(self, params) -> Any:
        res = await self.api.post(
            path=API_V1.MESSAGES,
            headers=self.headers,
            json=params,
        )
        return res

    async def send_bonus(self, json, params=None) -> Any:
        res = await self.api.post(path=API_V1.BONUS, headers=self.headers, json=json, params=params)
        return res

    async def get_operation_info(self, operation_id) -> Any:
        res = await self.api.get(
            path=f'{API_V1.OPERATIONS}/{operation_id}',
            headers=self.headers,
        )
        return res

    async def update_pool(self, pool_id: int, pool_params) -> Any:
        return await self.api.put(path=f'{API_V1.POOLS}/{pool_id}', headers=self.headers, json=pool_params)

    async def request_analytics(self, params) -> Any:
        res = await self.api.post(path=API_V1.ANALYTICS, headers=self.headers, json=params)
        return res

    async def get_balance(self) -> Any:
        """
        Return toloka amount of money in toloka account
        """
        res = await self.api.get(path=API_V1.BALANCE, headers=self.headers)
        return res

    async def _start_aggregating_solutions(self, params) -> Any:
        res = await self.api.post(path=API_V1.AGGREGATE, headers=self.headers, json=params)
        return res

    async def _get_aggregated_solutions(self, operation_id, params='') -> Any:
        res = await self.api.get(
            path=f'{API_V1.AGGREGATED_SOLUTIONS}/{operation_id}{params}',
            headers=self.headers,
        )
        return res

    async def _get_all_aggregated_solutions(self, operation_id, limit=500) -> list:
        results = []
        params = f'?limit={limit}&sort=task_id'
        aggr_results = await self._get_aggregated_solutions(operation_id, params)
        results.extend(aggr_results.get('items'))
        log.debug(len(results))
        while aggr_results['has_more']:
            last_item_task_id = aggr_results.get('items')[-1]['task_id']
            next_task_params = f'{params}&task_id_gt={last_item_task_id}'
            aggr_results = await self._get_aggregated_solutions(operation_id, next_task_params)
            results.extend(aggr_results['items'])
            log.debug(len(results))
        return results

    async def change_pool_priority(self, pool_id: int, priority: int):
        pool_settings: dict = await self.get_pool(pool_id)
        pool_settings['priority'] = priority
        res = self.patch_pool(pool_id, pool_settings)
        return res

    async def send_bonus_users(self, user_links: list, bonus: float, data: dict, private_comment: str = 'accepted'):
        user_bonuses = {}
        for user in user_links:
            if user in [u for u in user_bonuses.keys()]:
                user_bonuses[user] = user_bonuses.get(user) + 1
            else:
                user_bonuses[user] = 1

        bonus_params = {'async_mode': 'true', 'skip_invalid_items': 'true'}
        jsons = []
        for k, v in user_bonuses.items():
            jsons.append({
                'user_id': k,
                'amount': round(v * bonus, 2),
                'private_comment': private_comment,
                'public_title': {
                    'RU': f'{data["title"]}'
                },
                'public_message': {
                    'RU': f'{data["body"]}'
                },
            })
        log.info(f'total users: {len(jsons)}')
        log.info(f"total money: {sum(a['amount'] for a in jsons)}")
        operation = await self.send_bonus(jsons, bonus_params)
        if operation.get('id'):
            while True:
                status_ = await self.get_operation_info(operation['id'])
                status = status_.get('status')
                if status in ('PENDING', 'RUNNING'):
                    time.sleep(5)
                elif status == 'FAIL':
                    return False
                else:
                    break
            return True

    async def get_aggregated_solutions(self, pool_id: int, skill_id: int, field_names: list):
        """
        Return aggregated solutions 
        """
        params = {
            'pool_id': pool_id,
            'type': 'WEIGHTED_DYNAMIC_OVERLAP',
            'answer_weight_skill_id': skill_id,
            'fields': [{
                'name': n
            } for n in field_names]
        }

        operation = await self._start_aggregating_solutions(params)
        try:
            operation_id = operation['id']
        except KeyError:
            log.error('', exc_info=True)
            raise ValueError(f'{operation}')
        log.info(f'created operation {operation_id}')
        while (await self.get_operation_info(operation_id)).get('status') in ('PENDING', 'RUNNING'):
            await asyncio.sleep(5)
        solutions = await self._get_all_aggregated_solutions(operation_id)
        return solutions

    async def batch_upload_tasks(self, tasks: List[dict]):
        """
        Asynchrous uploading tasks to toloka
        """
        task_params = {'async_mode': 'true', 'allow_defaults': 'true'}
        operation = await self.create_task(tasks, task_params, timeout=20)
        while (await self.get_operation_info(operation['id'])).get('status') in ('PENDING', 'RUNNING'):
            await asyncio.sleep(5)
        return await self.get_operation_info(operation['id'])

    async def get_operation_result(self, operation) -> dict:
        """
        Get result from operation untill it done.
        """
        while (await self.get_operation_info(operation['id'])).get('status') in ('PENDING', 'RUNNING'):
            await asyncio.sleep(5)
        res = await self.get_operation_info(operation['id'])
        return res['details']

    async def change_pool_name(self, pool_id: int, name: str):
        pool_json = await self.get_pool(pool_id)
        old_name = pool_json['private_name']
        pool_json['private_name'] = name
        res = await self.update_pool(pool_id, pool_json)
        log.info(f'Pool {pool_id} {old_name} renamed to {name}')
        return res

    async def clone_pool_and_change_name(self, source_pool_id: Union[int, str], pool_name: str) -> dict:
        """
        Clone pool and change name.
        Returning new pool params.
        """
        operation = await self.clone_pool(int(source_pool_id))
        new_pool_id = await self.get_operation_result(operation)
        new_pool_id = new_pool_id['pool_id']
        await self.change_pool_name(new_pool_id, pool_name)
        new_pool = await self.get_pool(new_pool_id)
        return new_pool