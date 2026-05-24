"""
Cardano GraphQL load test scenarios.

Environment variables:
  QUERY_GROUPS   comma-separated groups to run: general,assets,transactions,addresses,staking
                 (default: all groups)
"""
from __future__ import annotations

import os
import random
from locust import HttpUser, between, task

GROUPS = set(os.environ.get("QUERY_GROUPS", "general,assets,transactions,addresses,staking").split(","))


class CardanoGraphQLUser(HttpUser):
    wait_time = between(0.5, 2)

    def on_start(self):
        self._tx_pool: list[str] = []
        self._addr_pool: list[str] = []
        self._asset_pool: list[tuple[str, str]] = []  # (fingerprint, policyId)
        self._req_count = 0
        self._refresh_pools()

    def _refresh_pools(self):
        r = self.gql(
            "~discovery.txs",
            "{ transactions(limit: 50, order_by: { includedAt: desc }) { hash inputs { address } } }",
        )
        if r:
            txs = r.get("data", {}).get("transactions", [])
            self._tx_pool = [t["hash"] for t in txs if t.get("hash")]
            self._addr_pool = [
                inp["address"]
                for t in txs
                for inp in t.get("inputs", [])
                if inp.get("address")
            ]

        r = self.gql(
            "~discovery.mints",
            """{ tokenMints(limit: 50, order_by: { transaction: { includedAt: desc } }) {
              asset { fingerprint policyId }
            } }""",
        )
        if r:
            self._asset_pool = [
                (m["asset"]["fingerprint"], m["asset"].get("policyId", ""))
                for m in r.get("data", {}).get("tokenMints", [])
                if m.get("asset", {}).get("fingerprint")
            ]

    def _tick(self):
        self._req_count += 1
        if self._req_count % 50 == 0:
            self._refresh_pools()

    def _rand_tx(self) -> str:
        return random.choice(self._tx_pool) if self._tx_pool else ""

    def _rand_addr(self) -> str:
        return random.choice(self._addr_pool) if self._addr_pool else ""

    def _rand_asset(self) -> tuple[str, str]:
        return random.choice(self._asset_pool) if self._asset_pool else ("", "")

    def gql(self, name: str, query: str, variables: dict | None = None) -> dict | None:
        with self.client.post(
            "/graphql",
            json={"query": query, "variables": variables or {}},
            name=name,
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}")
                return None
            try:
                data = resp.json()
            except Exception:
                resp.failure("Invalid JSON")
                return None
            if data.get("errors"):
                resp.failure(data["errors"][0].get("message", "GraphQL error"))
                return None
            resp.success()
            return data

    # ── general ────────────────────────────────────────────────────────────

    @task(5)
    def chain_tip(self):
        if "general" not in GROUPS:
            return
        self.gql(
            "cardano.tip",
            "{ cardano { tip { slotNo number forgedAt epoch { number } } } }",
        )

    @task(4)
    def db_meta(self):
        if "general" not in GROUPS:
            return
        self.gql(
            "cardanoDbMeta",
            "{ cardanoDbMeta { initialized syncPercentage } }",
        )

    @task(3)
    def recent_blocks(self):
        if "general" not in GROUPS:
            return
        self.gql(
            "blocks.recent",
            "{ blocks(limit: 5, order_by: { number: desc }) { number hash slotNo transactionsCount } }",
        )

    @task(3)
    def epoch_info(self):
        if "general" not in GROUPS:
            return
        self.gql(
            "epochs.latest",
            "{ epochs(limit: 1, order_by: { number: desc }) { number blocksCount transactionsCount output fees startedAt } }",
        )

    @task(2)
    def ada_supply(self):
        if "general" not in GROUPS:
            return
        self.gql(
            "ada.supply",
            "{ ada { supply { circulating max total } } }",
        )

    # ── assets ─────────────────────────────────────────────────────────────

    @task(3)
    def asset_list(self):
        if "assets" not in GROUPS:
            return
        self.gql(
            "assets.list",
            "{ assets(limit: 10) { fingerprint policyId assetName name decimals } }",
        )

    @task(4)
    def asset_by_fingerprint(self):
        if "assets" not in GROUPS:
            return
        fp, _ = self._rand_asset()
        if not fp:
            return
        self._tick()
        self.gql(
            "assets.byFingerprint",
            """query($fp: AssetFingerprint!) {
              assets(where: { fingerprint: { _eq: $fp } }) {
                fingerprint policyId assetName name decimals description metadataHash
                tokenMints(limit: 5, order_by: { transaction: { includedAt: desc } }) {
                  quantity transaction { hash includedAt }
                }
              }
            }""",
            {"fp": fp},
        )

    @task(2)
    def asset_by_policy(self):
        if "assets" not in GROUPS:
            return
        _, policy = self._rand_asset()
        if not policy:
            return
        self._tick()
        self.gql(
            "assets.byPolicy",
            """query($policy: Hash28Hex!) {
              assets(where: { policyId: { _eq: $policy } }) {
                fingerprint assetName name decimals
              }
            }""",
            {"policy": policy},
        )

    @task(3)
    def token_mints_recent(self):
        if "assets" not in GROUPS:
            return
        self.gql(
            "tokenMints.recent",
            """{ tokenMints(limit: 10, order_by: { transaction: { includedAt: desc } }, where: { asset: {} }) {
              quantity
              asset { fingerprint assetName name policyId }
              transaction { hash includedAt }
            } }""",
        )

    # ── transactions ───────────────────────────────────────────────────────

    @task(2)
    def transaction_by_hash(self):
        if "transactions" not in GROUPS:
            return
        h = self._rand_tx()
        if not h:
            return
        self._tick()
        self.gql(
            "transactions.byHash",
            """query($hash: Hash32Hex!) {
              transactions(where: { hash: { _eq: $hash } }) {
                hash fee size includedAt
                inputs { address value }
                outputs { address value }
                mint { asset { fingerprint assetName } quantity }
              }
            }""",
            {"hash": h},
        )

    # ── addresses ──────────────────────────────────────────────────────────

    @task(2)
    def payment_address(self):
        if "addresses" not in GROUPS:
            return
        addr = self._rand_addr()
        if not addr:
            return
        self._tick()
        self.gql(
            "paymentAddress.summary",
            """query($addr: String!) {
              paymentAddresses(addresses: [$addr]) {
                summary { assetBalances { quantity asset { fingerprint name } } }
              }
            }""",
            {"addr": addr},
        )

    # ── staking ────────────────────────────────────────────────────────────

    @task(1)
    def delegations(self):
        if "staking" not in GROUPS:
            return
        self.gql(
            "delegations.sample",
            "{ delegations(limit: 10) { address stakePoolId } }",
        )

    @task(1)
    def stake_pools(self):
        if "staking" not in GROUPS:
            return
        self.gql(
            "stakePools.list",
            "{ stakePools(limit: 5) { id pledge margin fixedCost } }",
        )
