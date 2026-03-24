from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.runtime.instruments import OptionContract
from app.runtime.settings import EngineConfig, HybridLegConfig
from app.services.engine.controller import EngineController
from app.services.engine.strategy import AddLot, CloseLadder, OpenLadder, StrategyEngine


class HybridEngineController(EngineController):
    """
    Hybrid engine:
    - Starts in BUY or SELL mode (config.hybrid.execution_mode)
    - Uses CALL/PUT preference from the base config (start_preference / instant_start / breakout logic)
    - Runs a ladder on one side (CALL or PUT) and flips between BUY <-> SELL on every TSL/stop hit
    - Flip is executed as a single net-reversal order on the *same* contract:
        BUY->SELL: SELL (close_qty + extra_open_qty)
        SELL->BUY: BUY  (close_qty + extra_open_qty)
    - Each leg has its own ladder params under config.hybrid.{call_buy,call_sell,put_buy,put_sell}
    """

    def __init__(self, config_store, instruments, *, underlying: str = "NIFTY", spot_candles: object | None = None) -> None:
        # Start with BUY; actual start kind is selected on `start()` from config.
        super().__init__(config_store, instruments, kind="BUY", underlying=underlying, spot_candles=spot_candles)
        self._hybrid_last_base_cfg: Optional[EngineConfig] = None

    # --- Display / mapping overrides -------------------------------------------------

    def _map_strategy_to_trade_side(self, strategy_side: str) -> str:
        # Hybrid always trades the same option type as the ladder side (CALL ladder -> CE, PUT ladder -> PE),
        # regardless of whether the current position is BUY or SELL.
        return str(strategy_side)

    def _display_side(self, strategy_side: str) -> str:
        # Show ladder side + current execution kind.
        k = "SELL" if self._kind == "SELL" else "BUY"
        return f"{str(strategy_side)}_{k}"

    # --- Config overrides ------------------------------------------------------------

    def _hybrid_leg(self, *, cfg: EngineConfig, side: str, kind: str) -> HybridLegConfig:
        side_u = str(side).upper()
        kind_u = "SELL" if str(kind).upper() == "SELL" else "BUY"
        h = getattr(cfg, "hybrid", None)
        if h is None:
            return HybridLegConfig()
        if side_u == "CALL":
            return h.call_sell if kind_u == "SELL" else h.call_buy
        return h.put_sell if kind_u == "SELL" else h.put_buy

    def _hybrid_cfg_for_leg(self, *, base_cfg: EngineConfig, side: str, kind: str) -> EngineConfig:
        leg = self._hybrid_leg(cfg=base_cfg, side=side, kind=kind)
        updates: dict = {}

        if leg.lots_per_add is not None:
            updates["lots_per_add"] = int(leg.lots_per_add)
        if leg.max_adds is not None:
            updates["max_adds"] = int(leg.max_adds)
        if leg.target_points is not None:
            updates["target_points"] = float(leg.target_points)
        if leg.initial_tsl_points is not None:
            updates["initial_tsl_points"] = float(leg.initial_tsl_points)
        if leg.sequence_tsl_diff_points is not None:
            updates["sequence_tsl_diff_points"] = float(leg.sequence_tsl_diff_points)

        # Hybrid flips kind automatically; do not use CALL<->PUT automation toggles.
        updates["full_automation"] = False
        updates["trade_direction_continue"] = False

        return base_cfg.model_copy(update=updates)

    def _cfg_for_engine(self, cfg: EngineConfig) -> EngineConfig:
        # Preserve sim override behavior from base controller, then apply hybrid leg overrides.
        base = super()._cfg_for_engine(cfg)
        self._hybrid_last_base_cfg = base

        side = self._engine.active_side
        if side is None:
            # If we're waiting for a breakout, a setup may already be formed (CALL/PUT) even before the ladder opens.
            setup = getattr(self._engine, "_setup", None)
            setup_side = getattr(setup, "side", None) if setup is not None else None
            if setup_side in ("CALL", "PUT"):
                side = setup_side
        if side is None and bool(getattr(base, "instant_start", False)):
            pref = str(getattr(base, "start_preference", "AUTO") or "AUTO").upper()
            if pref in ("CALL", "PUT"):
                side = pref
        if side is None:
            return base
        return self._hybrid_cfg_for_leg(base_cfg=base, side=str(side), kind=str(self._kind))

    # --- Contract selection ----------------------------------------------------------

    async def _select_option_contract(self, *, side: str, spot: float, now: datetime, cfg) -> OptionContract:
        """
        Hybrid uses the BUY-style strike selection for both BUY and SELL legs.
        (The same contract is then used for net-reversal flips.)
        """
        # Temporarily force BUY strike-selection logic while keeping HYBRID mapping.
        old_kind = self._kind
        try:
            self._kind = "BUY"
            return await super()._select_option_contract(side=side, spot=spot, now=now, cfg=cfg)
        finally:
            self._kind = old_kind

    # --- Lifecycle overrides ---------------------------------------------------------

    async def start(self, mode: str = "LIVE") -> None:  # type: ignore[override]
        cfg = await self._cfg_store.get()
        exec_mode = str(getattr(getattr(cfg, "hybrid", None), "execution_mode", "BUY") or "BUY").upper()
        self._kind = "SELL" if exec_mode == "SELL" else "BUY"
        self._engine = StrategyEngine.for_engine_kind(kind=self._kind)
        await super().start(mode=mode)  # type: ignore[arg-type]

    async def status(self):  # type: ignore[override]
        st = await super().status()
        st.engine_kind = "HYBRID"
        return st

    def sim_status(self) -> dict:  # type: ignore[override]
        st = super().sim_status()
        st["engine_kind"] = "HYBRID"
        return st

    async def on_config_updated(self, cfg: EngineConfig) -> None:
        if not self._running:
            return
        cfg_eng = self._cfg_for_engine(cfg)
        async with self._market_lock:
            self._engine.maybe_unlock_day(cfg_eng)
            self._engine.apply_live_config(cfg_eng)

            active_contract = self._active_contract
            last_spot = self._engine.last_tick
            if active_contract is None or last_spot is None:
                return
            ltp = self._option_ltps.get(active_contract.security_id)
            if ltp is None:
                return
            now = datetime.now(tz=getattr(last_spot.ts, "tzinfo", None))
            actions = self._engine.on_option_tick(premium_ltp=float(ltp), spot_ltp=float(last_spot.ltp), cfg=cfg_eng)
            await self._handle_actions(actions, spot=float(last_spot.ltp), cfg=cfg_eng, now=now)

    async def square_off_and_flip(self):  # type: ignore[override]
        raise RuntimeError("Hybrid engine does not support CALL<->PUT flip. It flips BUY<->SELL automatically on TSL.")

    async def flip_opposite_after_stop(self):  # type: ignore[override]
        raise RuntimeError("Hybrid engine does not wait for manual stop decisions.")

    async def continue_same_after_stop(self):  # type: ignore[override]
        raise RuntimeError("Hybrid engine does not wait for manual stop decisions.")

    # --- Hybrid stop flip ------------------------------------------------------------

    def _hybrid_other_kind(self, kind: str) -> str:
        return "BUY" if str(kind).upper() == "SELL" else "SELL"

    def _hybrid_display_for(self, *, side: str, kind: str) -> str:
        k = "SELL" if str(kind).upper() == "SELL" else "BUY"
        return f"{str(side).upper()}_{k}"

    async def _hybrid_flip_kind_order(
        self,
        *,
        side: str,
        spot: float,
        lots_open: int,
        cfg_base: EngineConfig,
        now: datetime,
    ) -> None:
        old_kind = str(self._kind).upper()
        new_kind = self._hybrid_other_kind(old_kind)
        old_engine = self._engine

        contract = self._active_contract
        if contract is None:
            # Fallback: nothing to flip.
            return

        # Close qty uses lots_open (includes adds) and optional broker lookup.
        old_qty = await self._resolve_close_qty(contract=contract, lots_open=lots_open, cfg=cfg_base)

        cfg_new = self._hybrid_cfg_for_leg(base_cfg=cfg_base, side=side, kind=new_kind)
        extra_lots = int(getattr(cfg_new, "lots_per_add", 1) or 1)
        if extra_lots < 1:
            extra_lots = 1
        extra_qty = max(1, contract.lot_size) * extra_lots

        txn = "SELL" if old_kind == "BUY" else "BUY"
        tag = f"open_{self._hybrid_display_for(side=side, kind=new_kind).lower()}"
        await self._enqueue_orders([(txn, contract.security_id, int(old_qty) + int(extra_qty), tag)], cfg=cfg_new)

        # Switch controller kind + strategy engine to the new leg.
        self._kind = "SELL" if new_kind == "SELL" else "BUY"
        new_engine = StrategyEngine.for_engine_kind(kind=self._kind)
        new_engine.loss_count = int(getattr(old_engine, "loss_count", 0) or 0)
        new_engine.last_tick = old_engine.last_tick
        new_engine._open_ladder(side=str(side).upper(), spot=float(spot), cfg=cfg_new)  # type: ignore[attr-defined]
        self._engine = new_engine

        # Reset live MTM tracker to the new leg (best-effort).
        if self._run_mode != "SIM":
            prem = self._option_ltps.get(contract.security_id)
            from app.services.engine.controller import _SimFill, _SimTrade  # local import to avoid circular typing

            self._mtm_active = _SimTrade(
                id=0,
                side=self._hybrid_display_for(side=side, kind=new_kind),
                strategy_side=str(side).upper(),
                trade_side=self._map_strategy_to_trade_side(str(side).upper()),
                kind=self._kind,
                contract=contract,
                fills=[
                    _SimFill(
                        ts=now,
                        spot=float(spot),
                        qty=int(extra_qty),
                        premium=None if prem is None else float(prem),
                    )
                ],
            )

    async def _hybrid_flip_kind_sim(
        self,
        *,
        side: str,
        spot: float,
        lots_open: int,
        reason: str,
        cfg_base: EngineConfig,
        now: datetime,
    ) -> None:
        contract = self._active_contract
        if contract is None:
            return

        old_kind = str(self._kind).upper()
        new_kind = self._hybrid_other_kind(old_kind)
        cfg_new = self._hybrid_cfg_for_leg(base_cfg=cfg_base, side=side, kind=new_kind)

        # Close current simulated trade.
        await super()._sim_close_ladder(
            side=str(side).upper(),
            spot=float(spot),
            lots_open=int(lots_open),
            reason=str(reason),
            flip_to=None,
            cfg=cfg_base,
            now=now,
        )

        # Switch controller kind + strategy engine BEFORE opening the next SIM trade,
        # so the new trade gets the correct display side and P&L direction.
        old_engine = self._engine
        self._kind = "SELL" if new_kind == "SELL" else "BUY"
        new_engine = StrategyEngine.for_engine_kind(kind=self._kind)
        new_engine.loss_count = int(getattr(old_engine, "loss_count", 0) or 0)
        new_engine.last_tick = old_engine.last_tick
        new_engine._open_ladder(side=str(side).upper(), spot=float(spot), cfg=cfg_new)  # type: ignore[attr-defined]
        self._engine = new_engine

        # Open the next leg on the same contract with the configured "extra" lots.
        await super()._sim_open_ladder_with_contract(
            side=str(side).upper(),
            spot=float(spot),
            contract=contract,
            cfg=cfg_new,
            now=now,
            unsubscribe_old=None,
        )

    async def _handle_actions(self, actions, *, spot: float, cfg, now: datetime) -> None:  # type: ignore[override]
        def _is_last_trade_exit(reason: str) -> bool:
            if not bool(getattr(cfg, "last_trade", False)):
                return False
            r = str(reason or "")
            return r == "target" or r.startswith("stop_")

        base_cfg = self._hybrid_last_base_cfg or cfg

        def _leg_cfg_for(side: str, kind: str) -> EngineConfig:
            return self._hybrid_cfg_for_leg(base_cfg=base_cfg, side=side, kind=kind)

        if self._run_mode == "SIM":
            stop_after = False
            for action in actions:
                if isinstance(action, OpenLadder):
                    cfg_leg = _leg_cfg_for(str(action.side).upper(), str(self._kind).upper())
                    await super()._sim_open_ladder(side=action.side, spot=action.spot, cfg=cfg_leg, now=now)
                elif isinstance(action, AddLot):
                    cfg_leg = _leg_cfg_for(str(action.side).upper(), str(self._kind).upper())
                    await super()._sim_add_lots(side=action.side, levels=action.levels, spot=action.spot, cfg=cfg_leg, now=now)
                elif isinstance(action, CloseLadder):
                    final = _is_last_trade_exit(str(action.reason))
                    is_stop = str(action.reason or "").startswith("stop_")
                    if is_stop and not final and not self._engine.day_locked and str(action.reason) != "stop_max_losses":
                        await self._hybrid_flip_kind_sim(
                            side=str(action.side).upper(),
                            spot=float(action.spot),
                            lots_open=int(action.lots_open),
                            reason=str(action.reason),
                            cfg_base=base_cfg,
                            now=now,
                        )
                        continue

                    await super()._sim_close_ladder(
                        side=action.side,
                        spot=action.spot,
                        lots_open=action.lots_open,
                        reason=action.reason,
                        flip_to=None,
                        cfg=base_cfg,
                        now=now,
                    )
                    if final:
                        stop_after = True
                        break
            if stop_after:
                self._engine.reset_day()
                await self.stop()
            return

        stop_after = False
        for action in actions:
            if stop_after:
                break
            if isinstance(action, OpenLadder):
                cfg_leg = _leg_cfg_for(str(action.side).upper(), str(self._kind).upper())
                await super()._open_ladder(side=action.side, spot=action.spot, cfg=cfg_leg, now=now)
            elif isinstance(action, AddLot):
                cfg_leg = _leg_cfg_for(str(action.side).upper(), str(self._kind).upper())
                await super()._add_lots(side=action.side, levels=action.levels, spot=action.spot, cfg=cfg_leg, now=now)
            elif isinstance(action, CloseLadder):
                final = _is_last_trade_exit(str(action.reason))
                is_stop = str(action.reason or "").startswith("stop_")
                if is_stop and not final and not self._engine.day_locked and str(action.reason) != "stop_max_losses":
                    await self._hybrid_flip_kind_order(
                        side=str(action.side).upper(),
                        spot=float(action.spot),
                        lots_open=int(action.lots_open),
                        cfg_base=base_cfg,
                        now=now,
                    )
                    continue

                await super()._close_ladder(
                    side=action.side,
                    spot=action.spot,
                    lots_open=action.lots_open,
                    reason=action.reason,
                    flip_to=None,
                    cfg=base_cfg,
                    now=now,
                )
                if final:
                    stop_after = True
                    break

        if stop_after:
            self._engine.reset_day()
            await self.stop()
