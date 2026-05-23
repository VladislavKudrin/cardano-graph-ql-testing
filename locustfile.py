"""
Cardano GraphQL load test scenarios.

Environment variables:
  LOAD_PROFILE   light | full (default: full)
  GQL_PAYMENT_ADDRESS  address to use for payment address queries
  GQL_TX_HASH          tx hash for transaction queries
  GQL_STAKE_ADDRESS    stake address for delegation/rewards queries
"""
from __future__ import annotations

import json
import os
from locust import HttpUser, between, task


PROFILE = os.environ.get("LOAD_PROFILE", "full")
PAYMENT_ADDRESS = os.environ.get(
    "GQL_PAYMENT_ADDRESS",
    "addr1qx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3n0d3vllmyqwsx5wktcd8cc3sq835lu7drv2xwl2wywfgse35a3x",
)
TX_HASH = os.environ.get(
    "GQL_TX_HASH",
    "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb",
)
STAKE_ADDRESS = os.environ.get(
    "GQL_STAKE_ADDRESS",
    "stake1uyehkck0lajq8gr28t9uxnuvgcqrc6070x3k9r8048z8y5gh6ffgw",
)


class CardanoGraphQLUser(HttpUser):
    wait_time = between(0.5, 2)

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

    # ── Lightweight queries (always included) ──────────────────────────────

    @task(5)
    def chain_tip(self):
        self.gql(
            "cardano.tip",
            "{ cardano { tip { slotNo number forgedAt epoch { number } } } }",
        )

    @task(4)
    def db_meta(self):
        self.gql(
            "cardanoDbMeta",
            "{ cardanoDbMeta { initialized syncPercentage } }",
        )

    @task(3)
    def recent_blocks(self):
        self.gql(
            "blocks.recent",
            "{ blocks(limit: 5, order_by: { number: desc }) { number hash slotNo transactionsCount } }",
        )

    @task(3)
    def epoch_info(self):
        self.gql(
            "epochs.latest",
            "{ epochs(limit: 1, order_by: { number: desc }) { number blocksCount transactionsCount output fees startedAt } }",
        )

    @task(2)
    def assets_list(self):
        self.gql(
            "assets.list",
            "{ assets(limit: 10) { fingerprint policyId assetName name decimals } }",
        )

    @task(2)
    def ada_supply(self):
        self.gql(
            "ada.supply",
            "{ ada { supply { circulating max total } } }",
        )

    # ── Full queries (skipped in light profile) ────────────────────────────

    @task(2)
    def payment_address(self):
        if PROFILE == "light":
            return
        self.gql(
            "paymentAddress.summary",
            """query($addr: String!) {
              paymentAddresses(addresses: [$addr]) {
                summary { assetBalances { quantity asset { fingerprint } } }
              }
            }""",
            {"addr": PAYMENT_ADDRESS},
        )

    @task(1)
    def transaction_by_hash(self):
        if PROFILE == "light":
            return
        self.gql(
            "transactions.byHash",
            """query($hash: Hash32Hex!) {
              transactions(where: { hash: { _eq: $hash } }) {
                hash fee size includedAt
                inputs { address value }
                outputs { address value }
              }
            }""",
            {"hash": TX_HASH},
        )

    @task(1)
    def delegations(self):
        if PROFILE == "light":
            return
        self.gql(
            "delegations.sample",
            "{ delegations(limit: 10, order_by: { transaction: { includedAt: desc } }) { stakeAddress { address } } }",
        )

    @task(1)
    def stake_pools(self):
        if PROFILE == "light":
            return
        self.gql(
            "stakePools.list",
            "{ stakePools(limit: 5) { id pledge margin cost } }",
        )
