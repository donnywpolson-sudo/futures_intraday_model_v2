(() => {
  "use strict";

  const PROTOCOL_VERSION = 3;
  const TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
  };
  const FAMILY_ORDER = [
    "Equity Index",
    "Energy",
    "Metals",
    "Rates",
    "FX",
    "Agriculture",
    "Livestock",
    "Other",
  ];
  const ALPHA_TIER_LABELS = {
    tier_1_core: "Tier 1 · Core",
    tier_2_additions: "Tier 2 · Additions",
    tier_3_additions: "Tier 3 · Additions",
  };
  const VISUAL_UPDATE_HZ = { efficient: 5, smooth: 10, high: 15 };
  const DEFAULT_VISUAL_UPDATE_MODE = "smooth";
  const POLL_EVENT_LIMIT = 100;
  const UNHEALTHY_POLL_LIMIT = 3;
  const RECOVERY_WINDOW_MS = 5000;
  const STATUS_CLASS = {
    LIVE: "live",
    WAITING: "waiting",
    STALE: "stale",
    ERROR: "error",
    CONNECTING: "waiting",
    RESOLVING: "waiting",
    BACKFILLING: "waiting",
    RECONNECTING: "stale",
    HISTORICAL_ONLY: "stale",
    STOPPED: "waiting",
    CHECKING: "waiting",
    CONFIRMATION_REQUIRED: "stale",
    WARMING: "waiting",
    PAUSED: "stale",
    COMPLETE: "live",
    PARTIAL: "stale",
  };
  const PREDICTION_REASON_LABELS = {
    MODEL_NOT_AUTHORIZED: "No authorized model is connected.",
    FEATURE_WARMUP_INCOMPLETE: "Waiting for enough completed bars and causal features.",
    DATA_INCOMPLETE: "Required input data is incomplete.",
    DATA_STALE: "The completed input bar is stale.",
    OUTSIDE_VALIDATED_SCOPE: "The selected market or interval is outside the validated scope.",
    OUTSIDE_DEMO_SCENARIO: "No synthetic forecast is defined for this demo market.",
    MODEL_ABSTAINED: "The synthetic example abstained instead of forcing a direction.",
    SYNTHETIC_DEMO_ERROR: "Synthetic error state for display testing.",
  };

  const state = {
    markets: [],
    statuses: new Map(),
    selectedMarket: "ES",
    timeframe: "1m",
    timeframes: Object.keys(TIMEFRAME_SECONDS),
    generation: 0,
    chart: null,
    candleSeries: null,
    volumeSeries: null,
    sessionMarkers: [],
    sessionBoundaryRenderQueued: false,
    sessionResizeObserver: null,
    chartResizeFrame: null,
    earliestBar: null,
    latestBar: null,
    bridgeReady: false,
    browserDemo: false,
    browserBars: new Map(),
    startupWatchdog: null,
    pollInFlight: false,
    pollTimer: null,
    visualUpdateConstrained: false,
    visualUpdateActive: true,
    lastSyncedVisualActive: null,
    unhealthyPolls: 0,
    healthySince: null,
    source: "waiting",
    barCount: 0,
    barCloses: new Map(),
    historyState: "IDLE",
    historyMessage: "",
    historyCategory: "",
    historyPopoverDismissedPlanId: null,
    historyCache: {
      state: "CHECKING",
      ready_markets: 0,
      total_markets: 33,
      queued_markets: 0,
      paused: false,
      plan_id: null,
      estimated_cost_usd: null,
      estimate_expires_at: null,
      message: "Checking one-week cache coverage",
    },
    fullscreen: false,
    mode: "live",
    contract: "",
    predictionCapability: { mode: "offline", synthetic: false, observation_only: true },
    prediction: null,
    predictionMarkers: null,
    dataHealth: null,
    executionCapability: null,
    manualTicketId: null,
    manualTicketState: "BLOCKED",
    alphaTierGroupingAvailable: false,
    alphaTierGroups: [],
    draggedMarketGroup: null,
    panelOpen: false,
    panelPreferenceExplicit: false,
    uiPreferences: {
      show_session_boundaries: true,
      show_volume: true,
      show_predictions: true,
      visual_update_mode: DEFAULT_VISUAL_UPDATE_MODE,
      market_grouping_mode: "sector",
      sector_group_order: [],
      alpha_tier_group_order: [],
      collapsed_sector_groups: [],
      collapsed_alpha_tier_groups: [],
    },
  };

  const elements = {
    marketList: document.getElementById("market-list"),
    marketSearch: document.getElementById("market-search"),
    marketGrouping: document.getElementById("market-grouping"),
    groupBySector: document.getElementById("group-by-sector"),
    groupByAlpha: document.getElementById("group-by-alpha"),
    groupReorderStatus: document.getElementById("group-reorder-status"),
    marketCount: document.getElementById("market-count"),
    overviewDot: document.getElementById("overview-dot"),
    overviewLabel: document.getElementById("overview-label"),
    instrumentSymbol: document.getElementById("instrument-symbol"),
    instrumentContract: document.getElementById("instrument-contract"),
    instrumentMeta: document.getElementById("instrument-meta"),
    focusState: document.getElementById("focus-state"),
    timeframeList: document.getElementById("timeframe-list"),
    fitChart: document.getElementById("fit-chart"),
    fullscreenToggle: document.getElementById("fullscreen-toggle"),
    layersToggle: document.getElementById("layers-toggle"),
    layersMenu: document.getElementById("layers-menu"),
    layerSessions: document.getElementById("layer-sessions"),
    layerVolume: document.getElementById("layer-volume"),
    layerPredictions: document.getElementById("layer-predictions"),
    smoothnessEfficient: document.getElementById("smoothness-efficient"),
    smoothnessSmooth: document.getElementById("smoothness-smooth"),
    smoothnessHigh: document.getElementById("smoothness-high"),
    localTime: document.getElementById("local-time"),
    workspaceContent: document.getElementById("workspace-content"),
    chart: document.getElementById("chart"),
    sessionBoundaries: document.getElementById("session-boundaries"),
    chartEmpty: document.getElementById("chart-empty"),
    chartEmptyTitle: document.getElementById("chart-empty-title"),
    chartEmptyDetail: document.getElementById("chart-empty-detail"),
    retryFocus: document.getElementById("retry-focus"),
    retryHistory: document.getElementById("retry-history"),
    dataHealthPill: document.getElementById("data-health-pill"),
    sourceState: document.getElementById("source-state"),
    footerDot: document.getElementById("footer-dot"),
    footerStatus: document.getElementById("footer-status"),
    renderRate: document.getElementById("render-rate"),
    quoteOpen: document.getElementById("quote-open"),
    quoteHigh: document.getElementById("quote-high"),
    quoteLow: document.getElementById("quote-low"),
    quoteClose: document.getElementById("quote-close"),
    quoteVolume: document.getElementById("quote-volume"),
    predictionRail: document.getElementById("prediction-rail"),
    predictionPanelToggle: document.getElementById("prediction-panel-toggle"),
    predictionMode: document.getElementById("prediction-mode"),
    predictionFreshness: document.getElementById("prediction-freshness"),
    predictionState: document.getElementById("prediction-state"),
    predictionReason: document.getElementById("prediction-reason"),
    probabilityLong: document.getElementById("probability-long"),
    probabilityFlat: document.getElementById("probability-flat"),
    probabilityShort: document.getElementById("probability-short"),
    probabilityLongValue: document.getElementById("probability-long-value"),
    probabilityFlatValue: document.getElementById("probability-flat-value"),
    probabilityShortValue: document.getElementById("probability-short-value"),
    predictionHorizon: document.getElementById("prediction-horizon"),
    predictionReturn: document.getElementById("prediction-return"),
    predictionTargetTime: document.getElementById("prediction-target-time"),
    predictionModel: document.getElementById("prediction-model"),
    predictionInputBar: document.getElementById("prediction-input-bar"),
    predictionGeneration: document.getElementById("prediction-generation"),
    predictionReasons: document.getElementById("prediction-reasons"),
    healthState: document.getElementById("health-state"),
    healthContract: document.getElementById("health-contract"),
    healthLastBar: document.getElementById("health-last-bar"),
    healthHistory: document.getElementById("health-history"),
    healthCoverage: document.getElementById("health-coverage"),
    healthBars: document.getElementById("health-bars"),
    healthContinuity: document.getElementById("health-continuity"),
    healthLoadedRange: document.getElementById("health-loaded-range"),
    dataHealthExplanation: document.getElementById("data-health-explanation"),
    historyCacheToggle: document.getElementById("history-cache-toggle"),
    historyCacheDot: document.getElementById("history-cache-dot"),
    historyCacheCount: document.getElementById("history-cache-count"),
    historyCachePopover: document.getElementById("history-cache-popover"),
    historyCacheClose: document.getElementById("history-cache-close"),
    historyCacheTitle: document.getElementById("history-cache-title"),
    historyCacheMessage: document.getElementById("history-cache-message"),
    historyCacheReady: document.getElementById("history-cache-ready"),
    historyCacheQueued: document.getElementById("history-cache-queued"),
    historyCacheCost: document.getElementById("history-cache-cost"),
    historyCacheExpiry: document.getElementById("history-cache-expiry"),
    historyCacheConfirm: document.getElementById("history-cache-confirm"),
    historyCachePause: document.getElementById("history-cache-pause"),
    historyCacheRetry: document.getElementById("history-cache-retry"),
    executionBanner: document.getElementById("execution-banner"),
    executionArmState: document.getElementById("execution-arm-state"),
    manualDemoStageControl: document.getElementById("manual-demo-stage-control"),
    manualDemoStage: document.getElementById("manual-demo-stage"),
    executionMode: document.getElementById("execution-mode"),
    executionOrigin: document.getElementById("execution-origin"),
    executionProfile: document.getElementById("execution-profile"),
    executionStage: document.getElementById("execution-stage"),
    executionConnection: document.getElementById("execution-connection"),
    executionEntitlement: document.getElementById("execution-entitlement"),
    executionAccount: document.getElementById("execution-account"),
    executionCosts: document.getElementById("execution-costs"),
    executionBrokerAge: document.getElementById("execution-broker-age"),
    executionMarketAge: document.getElementById("execution-market-age"),
    executionNewsAge: document.getElementById("execution-news-age"),
    executionSessionAge: document.getElementById("execution-session-age"),
    executionLimitAge: document.getElementById("execution-limit-age"),
    executionBalance: document.getElementById("execution-balance"),
    executionFloor: document.getElementById("execution-floor"),
    executionFloorDistance: document.getElementById("execution-floor-distance"),
    executionPositionCount: document.getElementById("execution-position-count"),
    executionOrderCount: document.getElementById("execution-order-count"),
    executionFillCount: document.getElementById("execution-fill-count"),
    executionBlockerList: document.getElementById("execution-blocker-list"),
    executionMaxQuantity: document.getElementById("execution-max-quantity"),
    executionFooterPill: document.getElementById("execution-footer-pill"),
    manualPreviewReadiness: document.getElementById("manual-preview-readiness"),
    manualAssistantReadiness: document.getElementById("manual-assistant-readiness"),
    providerApiReadiness: document.getElementById("provider-api-readiness"),
    automaticExecutionStatus: document.getElementById("automatic-execution-status"),
    manualAccountAlias: document.getElementById("manual-account-alias"),
    manualBalance: document.getElementById("manual-balance"),
    manualFloor: document.getElementById("manual-floor"),
    manualSessionPnl: document.getElementById("manual-session-pnl"),
    manualFloorLock: document.getElementById("manual-floor-lock"),
    manualOpenPositions: document.getElementById("manual-open-positions"),
    manualWorkingOrders: document.getElementById("manual-working-orders"),
    manualProtectiveOrders: document.getElementById("manual-protective-orders"),
    manualReconcile: document.getElementById("manual-reconcile"),
    manualReconcileStatus: document.getElementById("manual-reconcile-status"),
    executionSymbol: document.getElementById("execution-symbol"),
    executionSide: document.getElementById("execution-side"),
    executionOrderType: document.getElementById("execution-order-type"),
    executionQuantity: document.getElementById("execution-quantity"),
    executionEntryPrice: document.getElementById("execution-entry-price"),
    executionStopPrice: document.getElementById("execution-stop-price"),
    executionTargetPrice: document.getElementById("execution-target-price"),
    manualPrepareTicket: document.getElementById("manual-prepare-ticket"),
    manualPlannedRisk: document.getElementById("manual-planned-risk"),
    manualCostStatus: document.getElementById("manual-cost-status"),
    manualAggregateMicros: document.getElementById("manual-aggregate-micros"),
    manualCopyText: document.getElementById("manual-copy-text"),
    manualCopyOrder: document.getElementById("manual-copy-order"),
    manualMarkSubmitted: document.getElementById("manual-mark-submitted"),
    manualRecordPartial: document.getElementById("manual-record-partial"),
    manualRecordFill: document.getElementById("manual-record-fill"),
    manualConfirmProtection: document.getElementById("manual-confirm-protection"),
    manualRecordRejection: document.getElementById("manual-record-rejection"),
    manualRecordCancellation: document.getElementById("manual-record-cancellation"),
    manualRecordExit: document.getElementById("manual-record-exit"),
    manualReconcileTicket: document.getElementById("manual-reconcile-ticket"),
    manualStateUncertain: document.getElementById("manual-state-uncertain"),
    manualAbandon: document.getElementById("manual-abandon"),
    manualTicketState: document.getElementById("manual-ticket-state"),
    manualActualContract: document.getElementById("manual-actual-contract"),
    manualActualSide: document.getElementById("manual-actual-side"),
    manualActualFill: document.getElementById("manual-actual-fill"),
    manualActualQuantity: document.getElementById("manual-actual-quantity"),
    manualActualStop: document.getElementById("manual-actual-stop"),
    manualActualTarget: document.getElementById("manual-actual-target"),
    manualActualFees: document.getElementById("manual-actual-fees"),
    manualActualExit: document.getElementById("manual-actual-exit"),
    manualCompare: document.getElementById("manual-compare"),
    manualComparison: document.getElementById("manual-comparison"),
  };

  function executionLabel(value) {
    return String(value || "UNKNOWN").replaceAll("_", " ");
  }

  function renderExecution() {
    const capability = state.executionCapability;
    if (!capability) return;
    const simulator = capability.mode === "LOCAL_EXECUTION_SIMULATOR";
    const stageLabel = executionLabel(capability.account_stage);
    elements.executionBanner.textContent = simulator
      ? "LOCAL SIMULATOR — SYNTHETIC DATA / OPERATOR WORKFLOW DEMO"
      : `MFF ${stageLabel} — MANUAL TRADOVATE ENTRY REQUIRED`;
    elements.executionBanner.classList.toggle("simulator", simulator);
    elements.executionArmState.textContent = capability.armed ? "ARMED" : "DISARMED";
    elements.executionMode.textContent = executionLabel(capability.mode);
    elements.executionOrigin.textContent = executionLabel(capability.origin);
    elements.executionProfile.textContent = capability.profile_id;
    elements.executionStage.textContent = stageLabel;
    elements.manualDemoStageControl.hidden = state.mode !== "demo";
    if (state.mode === "demo") elements.manualDemoStage.value = capability.account_stage === "evaluation" ? "evaluation" : "sim_funded";
    elements.executionConnection.textContent = capability.provider_connection_opened ? "CONNECTED" : "NOT CONNECTED";
    elements.executionEntitlement.textContent = capability.entitlement_status;
    elements.executionAccount.textContent = capability.account_binding_present ? capability.account_binding_id : "UNSET";
    elements.executionCosts.textContent = capability.exact_costs_verified ? capability.cost_profile_id : "UNSET / UNVERIFIED";
    elements.executionBrokerAge.textContent = "NOT AVAILABLE";
    elements.executionMarketAge.textContent = state.dataHealth?.last_bar_time ? relativeTime(state.dataHealth.last_bar_time) : "WAITING";
    elements.executionNewsAge.textContent = "NOT BOUND";
    elements.executionSessionAge.textContent = "NOT BOUND";
    elements.executionLimitAge.textContent = "NOT BOUND";
    elements.executionMaxQuantity.textContent = "0";
    elements.manualPreviewReadiness.textContent = capability.manual_ticket_preview_available ? "AVAILABLE — NOT ENTRY READY" : "FALSE";
    elements.manualAssistantReadiness.textContent = capability.manual_assistant_readiness ? "TRUE" : "FALSE";
    elements.providerApiReadiness.textContent = capability.provider_api_readiness ? "TRUE" : "FALSE";
    elements.automaticExecutionStatus.textContent = capability.automatic_execution_authorized ? "AUTHORIZED" : "DISABLED";
    elements.executionFooterPill.textContent = `${executionLabel(capability.execution_capability)} — NO TRANSMISSION`;
    const blockers = Array.isArray(capability.blockers) ? capability.blockers : [];
    elements.executionBlockerList.replaceChildren(...blockers.map((reason) => {
      const item = document.createElement("li");
      item.textContent = executionLabel(reason);
      return item;
    }));
  }

  function manualNumber(element) {
    const value = Number(element.value);
    return Number.isFinite(value) ? value : null;
  }

  function manualSignalRoot(contract) {
    if (contract.startsWith("MES")) return "ES";
    if (contract.startsWith("MCL")) return "CL";
    if (contract.startsWith("M6E")) return "6E";
    return "ZN";
  }

  function manualJsonList(element, name) {
    let value;
    try {
      value = JSON.parse(element.value);
    } catch (_error) {
      throw new Error(`${name} must be valid JSON`);
    }
    if (!Array.isArray(value)) throw new Error(`${name} must be a JSON list`);
    return value;
  }

  function manualTicketPayload() {
    const capability = state.executionCapability || {};
    return {
      profile_id: capability.profile_id || "mff_rapid_eod_50k_2026_08_10",
      stage: capability.account_stage === "evaluation" ? "evaluation" : "sim_funded",
      account_alias: elements.manualAccountAlias.value.trim(),
      signal_instrument: manualSignalRoot(elements.executionSymbol.value.trim().toUpperCase()),
      execution_contract: elements.executionSymbol.value.trim().toUpperCase(),
      side: elements.executionSide.value,
      order_type: elements.executionOrderType.value,
      entry_price: manualNumber(elements.executionEntryPrice),
      stop_price: manualNumber(elements.executionStopPrice),
      target_price: manualNumber(elements.executionTargetPrice),
      quantity: Math.trunc(manualNumber(elements.executionQuantity) || 0),
      strategy_candidate_id: "coarse-3",
    };
  }

  function renderManualTicketResult(result) {
    if (!result?.ok || !result.ticket) {
      state.manualTicketId = null;
      state.manualTicketState = "BLOCKED";
      elements.manualTicketState.textContent = `BLOCKED • ${executionLabel(result?.error || "INVALID MANUAL TICKET")}`;
      renderManualControls();
      return;
    }
    const ticket = result.ticket;
    state.manualTicketId = ticket.ticket_id;
    state.manualTicketState = ticket.state;
    elements.manualActualContract.value = ticket.execution_contract || elements.executionSymbol.value;
    elements.manualActualSide.value = ticket.side || elements.executionSide.value;
    elements.executionMaxQuantity.textContent = String(ticket.authoritative_maximum_quantity ?? 0);
    elements.manualPreviewReadiness.textContent = ticket.manual_ticket_preview_available ? "AVAILABLE" : "FALSE";
    elements.manualAssistantReadiness.textContent = ticket.manual_assistant_readiness ? "TRUE" : "FALSE";
    elements.manualPlannedRisk.textContent = `${ticket.planned_stop_risk_usd ?? "—"} USD`;
    elements.manualAggregateMicros.textContent = `${ticket.projected_micro_equivalents ?? "—"} / 30 micros`;
    elements.manualCostStatus.textContent = "PROVISIONAL / NOT OFFICIAL";
    elements.manualCopyText.textContent = result.copy_text || "NO ORDER HAS BEEN TRANSMITTED BY FUTURESLIVECOCKPIT";
    elements.manualTicketState.textContent = `${executionLabel(ticket.state)} • ${executionLabel(ticket.authority)} • OPERATOR ENTRY REQUIRED`;
    const blockers = ticket.blocker_reason_codes || [];
    elements.executionBlockerList.replaceChildren(...(
      blockers.length ? blockers : ["NO_CURRENT_MANUAL_ENTRY_BLOCKER_PROVIDER_API_REMAINS_DISABLED"]
    ).map((reason) => {
        const item = document.createElement("li"); item.textContent = executionLabel(reason); return item;
      }));
    renderManualControls();
  }

  function renderManualControls() {
    const current = state.manualTicketState;
    const allowed = {
      copy: current === "READY_FOR_MANUAL_ENTRY",
      submitted: current === "READY_FOR_MANUAL_ENTRY",
      partial: current === "OPERATOR_REPORTED_SUBMITTED",
      filled: ["OPERATOR_REPORTED_SUBMITTED", "OPERATOR_REPORTED_PARTIALLY_FILLED"].includes(current),
      protected: current === "OPERATOR_REPORTED_FILLED",
      rejected: current === "OPERATOR_REPORTED_SUBMITTED",
      cancelled: current === "OPERATOR_REPORTED_SUBMITTED",
      closed: current === "OPERATOR_CONFIRMED_PROTECTED",
      reconciled: ["OPERATOR_REPORTED_REJECTED", "OPERATOR_REPORTED_CANCELLED", "OPERATOR_REPORTED_CLOSED", "STATE_UNCERTAIN"].includes(current),
      uncertain: ["BLOCKED", "READY_FOR_MANUAL_ENTRY", "OPERATOR_REPORTED_SUBMITTED", "OPERATOR_REPORTED_PARTIALLY_FILLED", "OPERATOR_REPORTED_FILLED", "OPERATOR_CONFIRMED_PROTECTED", "OPERATOR_REPORTED_CLOSED"].includes(current),
      abandoned: ["BLOCKED", "READY_FOR_MANUAL_ENTRY"].includes(current),
      compare: ["OPERATOR_REPORTED_PARTIALLY_FILLED", "OPERATOR_REPORTED_FILLED", "OPERATOR_CONFIRMED_PROTECTED", "OPERATOR_REPORTED_CLOSED", "STATE_UNCERTAIN"].includes(current),
    };
    elements.manualCopyOrder.disabled = !allowed.copy;
    elements.manualMarkSubmitted.disabled = !allowed.submitted;
    elements.manualRecordPartial.disabled = !allowed.partial;
    elements.manualRecordFill.disabled = !allowed.filled;
    elements.manualConfirmProtection.disabled = !allowed.protected;
    elements.manualRecordRejection.disabled = !allowed.rejected;
    elements.manualRecordCancellation.disabled = !allowed.cancelled;
    elements.manualRecordExit.disabled = !allowed.closed;
    elements.manualReconcileTicket.disabled = !allowed.reconciled;
    elements.manualStateUncertain.disabled = !allowed.uncertain;
    elements.manualAbandon.disabled = !allowed.abandoned;
    elements.manualCompare.disabled = !allowed.compare;
  }

  async function reconcileManualState() {
    const alias = elements.manualAccountAlias.value.trim();
    let openPositions;
    let workingOrders;
    let protectiveOrders;
    try {
      openPositions = manualJsonList(elements.manualOpenPositions, "Open positions");
      workingOrders = manualJsonList(elements.manualWorkingOrders, "Working entries");
      protectiveOrders = manualJsonList(elements.manualProtectiveOrders, "Protective orders");
    } catch (error) {
      elements.manualReconcileStatus.textContent = `STATE UNCERTAIN • ${String(error.message || error)}`;
      return;
    }
    const payload = {
      profile_id: state.executionCapability?.profile_id || "mff_rapid_eod_50k_2026_08_10",
      stage: state.executionCapability?.account_stage === "evaluation" ? "evaluation" : "sim_funded",
      account_alias: alias, nominal_plan_size_usd: 50000,
      realized_balance_usd: manualNumber(elements.manualBalance),
      active_eod_floor_usd: manualNumber(elements.manualFloor),
      floor_lock_status: elements.manualFloorLock.value, current_session_realized_pnl_usd: manualNumber(elements.manualSessionPnl),
      open_positions: openPositions, working_entry_orders: workingOrders, protective_orders: protectiveOrders, payout_state: "NOT_REPORTED",
      reconciliation_notes: "Operator reviewed MFF and Tradovate manual state.", confirmation: `RECONCILE ${alias}`,
    };
    const result = state.bridgeReady
      ? await window.pywebview.api.record_operator_account_snapshot(payload)
      : { ok: false, error: "DESKTOP_BRIDGE_REQUIRED" };
    elements.manualReconcileStatus.textContent = result.ok
      ? "OPERATOR REPORTED • CURRENT UNTIL STATE CHANGE OR RESTART"
      : `STATE UNCERTAIN • ${executionLabel(result.error)}`;
    if (result.ok) {
      const balance = manualNumber(elements.manualBalance);
      const floor = manualNumber(elements.manualFloor);
      elements.executionBalance.textContent = `${balance} USD • OPERATOR REPORTED`;
      elements.executionFloor.textContent = `${floor} USD • OPERATOR REPORTED`;
      elements.executionFloorDistance.textContent = `${balance - floor} USD`;
      elements.executionPositionCount.textContent = String(openPositions.length);
      elements.executionOrderCount.textContent = String(workingOrders.length + protectiveOrders.length);
    }
  }

  async function prepareManualTicket() {
    const payload = manualTicketPayload();
    const result = state.bridgeReady
      ? await window.pywebview.api.prepare_manual_ticket(payload)
      : { ok: false, error: "DESKTOP_BRIDGE_REQUIRED" };
    renderManualTicketResult(result);
  }

  async function manualTransition(target, report = {}) {
    if (!state.manualTicketId) {
      elements.manualTicketState.textContent = "BLOCKED • PREPARE A MANUAL TICKET FIRST";
      return;
    }
    const result = state.bridgeReady
      ? await window.pywebview.api.transition_manual_ticket({ ticket_id: state.manualTicketId, target, report })
      : { ok: false, error: "DESKTOP_BRIDGE_REQUIRED" };
    if (result.ok) {
      state.manualTicketState = result.state;
      if (result.state === "OPERATOR_REPORTED_FILLED") {
        elements.manualTicketState.textContent = "OPERATOR REPORTED FILLED • OPERATOR REPORTED • PROTECTION REQUIRED — NEW TICKETS BLOCKED • NOT BROKER CONFIRMED";
      } else if (result.state === "STATE_UNCERTAIN") {
        elements.manualTicketState.textContent = "STATE UNCERTAIN • OPERATOR REPORTED • NEW TICKETS BLOCKED • NOT BROKER CONFIRMED";
      } else {
        elements.manualTicketState.textContent = `${executionLabel(result.state)} • ${executionLabel(result.authority)} • NOT BROKER CONFIRMED`;
      }
      renderManualControls();
      if (["OPERATOR_REPORTED_PARTIALLY_FILLED", "OPERATOR_REPORTED_FILLED"].includes(result.state)) {
        elements.executionFillCount.textContent = "1";
      }
      elements.manualReconcileStatus.textContent = "STALE — RECONCILIATION REQUIRED • OPERATOR REPORTED";
    } else {
      elements.manualTicketState.textContent = `STATE UNCERTAIN • ${executionLabel(result.error)}`;
    }
  }

  async function compareManualActual() {
    if (!state.manualTicketId) return;
    const report = {
      actual_contract: elements.manualActualContract.value.trim().toUpperCase(),
      actual_side: elements.manualActualSide.value,
      actual_quantity: Math.trunc(manualNumber(elements.manualActualQuantity) || 0),
      actual_fill_price: manualNumber(elements.manualActualFill),
      actual_stop: manualNumber(elements.manualActualStop),
      actual_target: manualNumber(elements.manualActualTarget),
      actual_fees: manualNumber(elements.manualActualFees),
    };
    if (!state.bridgeReady) {
      elements.manualComparison.textContent = "STATE UNCERTAIN • DESKTOP BRIDGE REQUIRED";
      return;
    }
    const result = await window.pywebview.api.compare_manual_ticket({ ticket_id: state.manualTicketId, report });
    if (result?.ok && result.comparison?.resulting_state) {
      state.manualTicketState = result.comparison.resulting_state;
      if (state.manualTicketState === "STATE_UNCERTAIN") {
        elements.manualTicketState.textContent = "STATE UNCERTAIN • OPERATOR REPORTED • NEW TICKETS BLOCKED • NOT BROKER CONFIRMED";
        elements.manualReconcileStatus.textContent = "STALE — RECONCILIATION REQUIRED • OPERATOR REPORTED";
      }
      renderManualControls();
    }
    elements.manualComparison.textContent = result.ok
      ? `OPERATOR REPORTED • Slippage ${result.comparison.entry_slippage_ticks} ticks • Alerts: ${(result.comparison.alerts || []).map(executionLabel).join(", ") || "none"}`
      : `STATE UNCERTAIN • ${executionLabel(result.error)}`;
  }

  function statusClass(value) {
    return STATUS_CLASS[String(value || "WAITING").toUpperCase()] || "waiting";
  }

  function setDot(element, value) {
    element.className = `status-dot ${statusClass(value)}`;
  }

  function formatPrice(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    const number = Number(value);
    const absolute = Math.abs(number);
    const digits = absolute >= 1000 ? 2 : absolute >= 10 ? 3 : 4;
    return number.toLocaleString(undefined, {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }

  function formatVolume(value) {
    if (value === null || value === undefined) return "—";
    const number = Number(value);
    if (number >= 1_000_000) return `${(number / 1_000_000).toFixed(1)}M`;
    if (number >= 1_000) return `${(number / 1_000).toFixed(1)}K`;
    return Math.round(number).toLocaleString();
  }

  function initializeChart() {
    if (state.chart || !window.LightweightCharts) return;
    const LWC = window.LightweightCharts;
    const localTime = window.CockpitTime;
    state.chart = LWC.createChart(elements.chart, {
      autoSize: false,
      layout: {
        background: { type: LWC.ColorType ? LWC.ColorType.Solid : "solid", color: "transparent" },
        textColor: "#8493a8",
        fontFamily: 'Inter, ui-sans-serif, system-ui, "Segoe UI", sans-serif',
        fontSize: 11,
        attributionLogo: true,
        panes: {
          separatorColor: "rgba(139, 163, 193, 0.12)",
          separatorHoverColor: "rgba(116, 167, 255, 0.25)",
          enableResize: true,
        },
      },
      ...(localTime ? {
        localization: {
          locale: navigator.language,
          timeFormatter: (time) => localTime.formatLocalCrosshairTime(time, navigator.language),
        },
      } : {}),
      grid: {
        vertLines: { color: "rgba(139, 163, 193, 0.055)" },
        horzLines: { color: "rgba(139, 163, 193, 0.055)" },
      },
      crosshair: {
        mode: LWC.CrosshairMode ? LWC.CrosshairMode.Normal : 0,
        vertLine: { color: "rgba(145, 164, 190, 0.45)", width: 1, style: 3, labelBackgroundColor: "#273448" },
        horzLine: { color: "rgba(145, 164, 190, 0.4)", width: 1, style: 3, labelBackgroundColor: "#273448" },
      },
      rightPriceScale: {
        borderColor: "rgba(139, 163, 193, 0.12)",
        scaleMargins: { top: 0.12, bottom: 0.08 },
        minimumWidth: 78,
      },
      timeScale: {
        borderColor: "rgba(139, 163, 193, 0.12)",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 8,
        barSpacing: 8,
        minBarSpacing: 0.5,
        shiftVisibleRangeOnNewBar: true,
        enableConflation: true,
        tickMarkFormatter: localTime
          ? (time, tickMarkType, locale) => localTime.formatLocalTickMark(time, tickMarkType, locale)
          : undefined,
      },
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
    });

    if (state.chart.addSeries && LWC.CandlestickSeries) {
      state.candleSeries = state.chart.addSeries(LWC.CandlestickSeries, {
        upColor: "#35c7a0",
        downColor: "#f06f79",
        wickUpColor: "#35c7a0",
        wickDownColor: "#f06f79",
        borderVisible: false,
        priceLineColor: "rgba(116, 167, 255, 0.55)",
        priceLineWidth: 1,
      });
      state.volumeSeries = state.chart.addSeries(
        LWC.HistogramSeries,
        { base: 0, priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false },
        1,
      );
      state.volumeSeries.priceScale().applyOptions({
        autoScale: true,
        scaleMargins: { top: 0.16, bottom: 0 },
      });
      const panes = state.chart.panes ? state.chart.panes() : [];
      if (panes[0] && panes[0].setStretchFactor) panes[0].setStretchFactor(4);
      if (panes[1] && panes[1].setStretchFactor) panes[1].setStretchFactor(1);
    } else {
      state.candleSeries = state.chart.addCandlestickSeries({
        upColor: "#35c7a0",
        downColor: "#f06f79",
        wickUpColor: "#35c7a0",
        wickDownColor: "#f06f79",
        borderVisible: false,
      });
      state.volumeSeries = state.chart.addHistogramSeries({
        base: 0,
        priceFormat: { type: "volume" },
        priceScaleId: "volume",
      });
      state.volumeSeries.priceScale().applyOptions({
        autoScale: true,
        scaleMargins: { top: 0.82, bottom: 0 },
      });
    }
    if (LWC.createSeriesMarkers && state.candleSeries) {
      state.predictionMarkers = LWC.createSeriesMarkers(state.candleSeries, []);
    }
    if (state.volumeSeries?.applyOptions) {
      state.volumeSeries.applyOptions({ visible: state.uiPreferences.show_volume });
    }

    state.chart.subscribeCrosshairMove((parameter) => {
      if (!parameter || !parameter.time || !parameter.seriesData) {
        updateQuote(state.latestBar);
        return;
      }
      const candle = parameter.seriesData.get(state.candleSeries);
      if (candle) updateQuote(candle);
    });
    const timeScale = state.chart.timeScale();
    if (timeScale.subscribeVisibleLogicalRangeChange) {
      timeScale.subscribeVisibleLogicalRangeChange(queueSessionBoundaryRender);
    }
    if (window.ResizeObserver) {
      state.sessionResizeObserver = new ResizeObserver(queueChartResize);
      state.sessionResizeObserver.observe(elements.chart);
    }
    queueChartResize();
  }

  function resizeChartToContainer() {
    state.chartResizeFrame = null;
    if (!state.chart) return;
    const width = Math.floor(elements.chart.clientWidth);
    const height = Math.floor(elements.chart.clientHeight);
    if (width <= 0 || height <= 0) return;
    state.chart.resize(width, height, true);
    queueSessionBoundaryRender();
  }

  function queueChartResize() {
    if (state.chartResizeFrame !== null) return;
    state.chartResizeFrame = window.requestAnimationFrame(resizeChartToContainer);
  }

  function settleFullscreenChartLayout() {
    resizeChartToContainer();
    if (state.chart) state.chart.timeScale().fitContent();
  }

  function sessionBoundaryKind(marker) {
    const label = String(marker?.text || "").trim().toLowerCase();
    if (label === "rth") return "rth";
    if (label === "g" || label === "globex") return "globex";
    return null;
  }

  function renderSessionBoundaries() {
    state.sessionBoundaryRenderQueued = false;
    if (!state.chart || !elements.sessionBoundaries) return;
    elements.sessionBoundaries.replaceChildren();
    if (!state.uiPreferences.show_session_boundaries) return;
    const width = elements.chart.clientWidth;
    if (!width || state.timeframe === "1d") return;
    const timeScale = state.chart.timeScale();
    state.sessionMarkers.forEach((marker) => {
      const kind = sessionBoundaryKind(marker);
      const coordinate = timeScale.timeToCoordinate(Number(marker.time));
      if (!kind || !Number.isFinite(coordinate) || coordinate < 0 || coordinate > width - 72) return;
      const boundary = document.createElement("div");
      boundary.className = `session-boundary ${kind}`;
      if (coordinate > width - 160) boundary.classList.add("align-left");
      boundary.style.left = `${Math.round(coordinate)}px`;
      const label = document.createElement("span");
      label.className = "session-boundary-label";
      label.textContent = kind === "rth" ? "RTH open" : "Globex open";
      boundary.appendChild(label);
      elements.sessionBoundaries.appendChild(boundary);
    });
  }

  function queueSessionBoundaryRender() {
    if (state.sessionBoundaryRenderQueued) return;
    state.sessionBoundaryRenderQueued = true;
    window.requestAnimationFrame(renderSessionBoundaries);
  }

  function updateQuote(bar) {
    if (!bar) {
      [
        elements.quoteOpen,
        elements.quoteHigh,
        elements.quoteLow,
        elements.quoteClose,
        elements.quoteVolume,
      ].forEach((element) => { element.textContent = "—"; });
      return;
    }
    elements.quoteOpen.textContent = formatPrice(bar.open);
    elements.quoteHigh.textContent = formatPrice(bar.high);
    elements.quoteLow.textContent = formatPrice(bar.low);
    elements.quoteClose.textContent = formatPrice(bar.close);
    elements.quoteVolume.textContent = formatVolume(bar.volume);
  }

  function formatAge(timestamp) {
    if (timestamp === null || timestamp === undefined || timestamp === "" || !Number.isFinite(Number(timestamp))) return "—";
    const seconds = Math.max(0, Math.floor(Date.now() / 1000 - Number(timestamp)));
    if (seconds < 5) return "Now";
    if (seconds < 60) return `${seconds}s ago`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h ago`;
  }

  function formatEventTime(timestamp) {
    if (timestamp === null || timestamp === undefined || timestamp === "" || !Number.isFinite(Number(timestamp))) return "—";
    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(new Date(Number(timestamp) * 1000));
  }

  function loadedChartRange() {
    const start = Number(state.earliestBar?.time);
    const end = Number(state.latestBar?.time);
    if (!Number.isFinite(start) || !Number.isFinite(end)) return "No visible bars";
    return `${formatEventTime(start)} – ${formatEventTime(end)} local`;
  }

  function setProbability(name, value) {
    const bar = elements[`probability${name}`];
    const label = elements[`probability${name}Value`];
    const probability = Number(value);
    const available = Number.isFinite(probability);
    bar.style.width = available ? `${Math.max(0, Math.min(1, probability)) * 100}%` : "0%";
    label.textContent = available ? `${Math.round(probability * 100)}%` : "—";
  }

  function formatNativeMove(expectedReturn, inputBarTime) {
    const referenceClose = state.barCloses.get(Number(inputBarTime));
    if (!Number.isFinite(expectedReturn) || !Number.isFinite(referenceClose)) return "—";
    const move = expectedReturn * referenceClose;
    const absoluteReference = Math.abs(referenceClose);
    const digits = absoluteReference >= 1000 ? 2 : absoluteReference >= 10 ? 3 : 4;
    const formatted = move.toLocaleString(undefined, {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
    return `${move > 0 ? "+" : ""}${formatted} pts`;
  }

  function formatTargetWindow(horizonSeconds) {
    const seconds = Number(horizonSeconds);
    if (!Number.isFinite(seconds) || seconds <= 0) return "\u2014";
    if (seconds % 3600 === 0) return `Next ${seconds / 3600}h`;
    if (seconds % 60 === 0) return `Next ${seconds / 60}m`;
    return `Next ${Math.round(seconds)}s`;
  }

  function renderPredictionMarker() {
    if (!state.predictionMarkers?.setMarkers) return;
    const prediction = state.prediction;
    const forecast = prediction?.forecast;
    if (
      !state.uiPreferences.show_predictions ||
      prediction?.state !== "READY" ||
      !forecast ||
      !Number.isFinite(Number(prediction.input_bar_time))
    ) {
      state.predictionMarkers.setMarkers([]);
      return;
    }
    const direction = String(forecast.direction || "FLAT").toUpperCase();
    const marker = {
      time: Number(prediction.input_bar_time),
      position: direction === "LONG" ? "belowBar" : "aboveBar",
      shape: direction === "LONG" ? "arrowUp" : direction === "SHORT" ? "arrowDown" : "square",
      color: direction === "LONG" ? "#35c7a0" : direction === "SHORT" ? "#f06f79" : "#9ba9bb",
      size: 0.55,
    };
    state.predictionMarkers.setMarkers([marker]);
  }

  function renderPrediction() {
    const prediction = state.prediction;
    const synthetic = Boolean(prediction?.synthetic || state.predictionCapability.synthetic);
    elements.predictionMode.textContent = synthetic ? "SYNTHETIC DEMO" : "LIVE MODE";
    elements.predictionMode.className = `forecast-mode ${synthetic ? "synthetic" : "offline"}`;
    elements.predictionFreshness.textContent = prediction ? formatAge(prediction.prediction_time) : "No event";

    const stateName = String(prediction?.state || (synthetic ? "WARMING_UP" : "OFFLINE")).toUpperCase();
    const forecast = prediction?.forecast || null;
    const direction = String(forecast?.direction || "").toUpperCase();
    let stateLabel = "NO PREDICTION";
    if (stateName === "READY") stateLabel = `${direction || "FLAT"} BIAS`;
    else if (stateName === "WARMING_UP") stateLabel = "WARMING UP";
    elements.predictionState.textContent = stateLabel;
    const stateClass = stateName === "READY" ? direction.toLowerCase() : stateName.toLowerCase().replace("_up", "");
    elements.predictionState.className = `forecast-state ${stateClass}`;

    const reasons = Array.isArray(prediction?.reason_codes) ? prediction.reason_codes : [];
    elements.predictionReason.textContent = stateName === "READY"
      ? "Synthetic example generated from a completed demo bar."
      : PREDICTION_REASON_LABELS[reasons[0]] || (synthetic
        ? "Waiting for a deterministic synthetic scenario."
        : "No authorized model is connected.");

    const probabilities = forecast?.probabilities || {};
    setProbability("Long", probabilities.long);
    setProbability("Flat", probabilities.flat);
    setProbability("Short", probabilities.short);
    const horizonSeconds = Number(forecast?.horizon_seconds);
    elements.predictionHorizon.textContent = formatTargetWindow(horizonSeconds);
    const expectedReturn = Number(forecast?.expected_return);
    elements.predictionReturn.textContent = formatNativeMove(
      expectedReturn,
      prediction?.input_bar_time,
    );
    const predictionTime = Number(prediction?.prediction_time);
    elements.predictionTargetTime.textContent = (
      Number.isFinite(predictionTime) && Number.isFinite(horizonSeconds)
    ) ? formatEventTime(predictionTime + horizonSeconds) : "\u2014";
    const model = prediction?.model;
    elements.predictionModel.textContent = model ? `${model.id} v${model.version}` : "—";
    elements.predictionInputBar.textContent = formatEventTime(prediction?.input_bar_time);
    elements.predictionGeneration.textContent = Number.isFinite(Number(prediction?.generation))
      ? String(prediction.generation)
      : "—";
    elements.predictionReasons.textContent = reasons.length ? reasons.join(", ") : "—";
    renderPredictionMarker();
  }

  function renderDataHealth() {
    const health = state.dataHealth;
    const healthState = String(health?.state || "UNKNOWN").toUpperCase();
    const healthClass = healthState.toLowerCase();
    elements.dataHealthPill.textContent = `Data ${healthState.toLowerCase()}`;
    elements.dataHealthPill.className = `data-health-pill ${healthClass}`;
    elements.healthState.textContent = healthState;
    elements.healthState.className = healthClass;
    elements.healthContract.textContent = health?.contract || state.contract || "—";
    elements.healthLastBar.textContent = health && health.last_bar_time !== null && health.last_bar_time !== undefined && Number.isFinite(Number(health.last_bar_time))
      ? `${formatEventTime(health.last_bar_time)} (${formatAge(health.last_bar_time)})`
      : "—";
    const history = health?.history;
    elements.healthHistory.textContent = history?.state ? String(history.state).replaceAll("_", " ") : "—";
    elements.healthCoverage.textContent = history
      ? `${Number(history.coverage_hours || 0).toFixed(1)} / ${Number(history.requested_hours || 0).toFixed(0)}h`
      : "—";
    elements.healthBars.textContent = history ? Number(history.bar_count || 0).toLocaleString() : "—";
    const continuity = health?.continuity;
    if (!continuity || continuity.state === "NOT_EVALUATED") {
      elements.healthContinuity.textContent = "Not evaluated";
    } else if (continuity.state === "PASS") {
      elements.healthContinuity.textContent = "No gaps";
    } else {
      const count = Number(continuity.unexpected_gap_count || 0);
      const largest = Number(continuity.largest_gap_seconds || 0);
      elements.healthContinuity.textContent = `${count} gap${count === 1 ? "" : "s"} · max ${Math.round(largest / 60)}m`;
    }
    elements.healthLoadedRange.textContent = loadedChartRange();
    const cacheState = String(state.historyCache?.state || "CHECKING").toUpperCase();
    const range = loadedChartRange();
    const age = health && health.last_bar_time !== null && health.last_bar_time !== undefined && Number.isFinite(Number(health.last_bar_time))
      ? formatAge(health.last_bar_time)
      : "unknown";
    if (cacheState === "CONFIRMATION_REQUIRED") {
      elements.dataHealthExplanation.textContent = `Showing cached data only (${range}). No history download has started. Review the estimate and approve the update to load missing recent days; the newest loaded bar is ${age}.`;
    } else if (["WARMING", "PAUSED"].includes(cacheState)) {
      elements.dataHealthExplanation.textContent = `Showing ${range} while missing one-week history is ${cacheState === "PAUSED" ? "paused" : "updated in the background"}.`;
    } else if (healthState === "CURRENT") {
      elements.dataHealthExplanation.textContent = `The focused stream and requested history are current. Loaded range: ${range}.`;
    } else if (health) {
      elements.dataHealthExplanation.textContent = `The chart is not current. Loaded range: ${range}; newest bar: ${age}. History state: ${String(history?.state || "unknown").replaceAll("_", " ").toLowerCase()}.`;
    } else {
      elements.dataHealthExplanation.textContent = "Waiting for chart coverage details.";
    }
  }

  function clearSelectionContext() {
    state.prediction = null;
    state.dataHealth = null;
    state.barCloses.clear();
    renderPrediction();
    renderDataHealth();
  }

  function setPanelOpen(open, { persist = false } = {}) {
    state.panelOpen = Boolean(open);
    elements.workspaceContent.classList.toggle("panel-collapsed", !state.panelOpen);
    elements.workspaceContent.style.gridTemplateColumns = state.panelOpen
      ? "minmax(0, 1fr) 304px"
      : "minmax(0, 1fr) 44px";
    elements.predictionRail.setAttribute("aria-expanded", String(state.panelOpen));
    elements.predictionPanelToggle.setAttribute("aria-expanded", String(state.panelOpen));
    elements.predictionPanelToggle.setAttribute(
      "aria-label",
      state.panelOpen ? "Collapse shadow forecast" : "Expand shadow forecast",
    );
    if (persist) {
      state.panelPreferenceExplicit = true;
      state.uiPreferences.prediction_panel_open = state.panelOpen;
      persistUiPreferences();
    }
    window.requestAnimationFrame(queueSessionBoundaryRender);
  }

  function applyLayerVisibility() {
    elements.layerSessions.checked = state.uiPreferences.show_session_boundaries;
    elements.layerVolume.checked = state.uiPreferences.show_volume;
    elements.layerPredictions.checked = state.uiPreferences.show_predictions;
    if (state.volumeSeries?.applyOptions) {
      state.volumeSeries.applyOptions({ visible: state.uiPreferences.show_volume });
    }
    queueSessionBoundaryRender();
    renderPredictionMarker();
  }

  function visualUpdateMode() {
    return Object.hasOwn(VISUAL_UPDATE_HZ, state.uiPreferences.visual_update_mode)
      ? state.uiPreferences.visual_update_mode
      : DEFAULT_VISUAL_UPDATE_MODE;
  }

  function effectiveVisualUpdateHz() {
    return document.hidden || state.visualUpdateConstrained
      ? VISUAL_UPDATE_HZ.efficient
      : VISUAL_UPDATE_HZ[visualUpdateMode()];
  }

  function renderVisualUpdateState() {
    const mode = visualUpdateMode();
    elements.smoothnessEfficient.checked = mode === "efficient";
    elements.smoothnessSmooth.checked = mode === "smooth";
    elements.smoothnessHigh.checked = mode === "high";
    const effective = effectiveVisualUpdateHz();
    const adaptive = state.visualUpdateConstrained && !document.hidden && mode !== "efficient";
    elements.renderRate.textContent = `≤ ${effective} visual updates/sec${adaptive ? " · adaptive" : ""}`;
  }

  async function syncVisualUpdateActivity() {
    const active = !document.hidden && !state.visualUpdateConstrained;
    state.visualUpdateActive = active;
    renderVisualUpdateState();
    if (
      state.browserDemo
      || !state.bridgeReady
      || state.lastSyncedVisualActive === active
      || !window.pywebview?.api?.set_visual_update_active
    ) return;
    state.lastSyncedVisualActive = active;
    try {
      await window.pywebview.api.set_visual_update_active(active);
    } catch (_error) {
      state.lastSyncedVisualActive = null;
    }
  }

  function applyUiPreferences(preferences) {
    const values = preferences && typeof preferences === "object" ? preferences : {};
    ["show_session_boundaries", "show_volume", "show_predictions"].forEach((key) => {
      if (typeof values[key] === "boolean") state.uiPreferences[key] = values[key];
    });
    if (Object.hasOwn(VISUAL_UPDATE_HZ, values.visual_update_mode)) {
      state.uiPreferences.visual_update_mode = values.visual_update_mode;
    }
    if (["sector", "alpha_tier"].includes(values.market_grouping_mode)) {
      state.uiPreferences.market_grouping_mode = values.market_grouping_mode;
    }
    [
      "sector_group_order",
      "alpha_tier_group_order",
      "collapsed_sector_groups",
      "collapsed_alpha_tier_groups",
    ].forEach((key) => {
      if (Array.isArray(values[key])) state.uiPreferences[key] = [...new Set(values[key].map(normalizeGroupId))];
    });
    if (!state.alphaTierGroupingAvailable && state.uiPreferences.market_grouping_mode === "alpha_tier") {
      state.uiPreferences.market_grouping_mode = "sector";
    }
    state.panelPreferenceExplicit = typeof values.prediction_panel_open === "boolean";
    const defaultOpen = window.innerWidth >= 1440;
    setPanelOpen(state.panelPreferenceExplicit ? values.prediction_panel_open : defaultOpen);
    applyLayerVisibility();
    updateGroupingControls();
    renderVisualUpdateState();
    syncVisualUpdateActivity();
  }

  async function persistUiPreferences() {
    if (state.browserDemo || !state.bridgeReady || !window.pywebview?.api?.set_ui_preferences) return;
    try {
      await window.pywebview.api.set_ui_preferences(state.uiPreferences);
    } catch (_error) {
      // Preferences are optional and never interrupt chart or feed updates.
    }
  }

  function resetChartView() {
    if (!state.chart) return;
    if (state.candleSeries?.priceScale) {
      state.candleSeries.priceScale().applyOptions({ autoScale: true });
    }
    if (state.volumeSeries?.priceScale) {
      state.volumeSeries.priceScale().applyOptions({ autoScale: true });
    }
    state.chart.timeScale().fitContent();
    queueSessionBoundaryRender();
  }

  function scheduleChartReset() {
    resetChartView();
    const generation = state.generation;
    const timeframe = state.timeframe;
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        if (generation === state.generation && timeframe === state.timeframe) {
          resetChartView();
        }
      });
    });
  }

  function followLatestBar() {
    if (!state.chart) return;
    if (state.candleSeries?.priceScale) {
      state.candleSeries.priceScale().applyOptions({ autoScale: true });
    }
    if (state.volumeSeries?.priceScale) {
      state.volumeSeries.priceScale().applyOptions({ autoScale: true });
    }
    state.chart.timeScale().scrollToRealTime();
    queueSessionBoundaryRender();
  }

  function renderFullscreenState() {
    const label = elements.fullscreenToggle.querySelector("span");
    elements.fullscreenToggle.classList.toggle("active", state.fullscreen);
    elements.fullscreenToggle.setAttribute("aria-pressed", String(state.fullscreen));
    elements.fullscreenToggle.setAttribute(
      "aria-label",
      state.fullscreen ? "Exit full screen" : "Enter full screen",
    );
    elements.fullscreenToggle.title = state.fullscreen
      ? "Exit full screen (F11)"
      : "Enter full screen (F11)";
    if (label) label.textContent = state.fullscreen ? "Exit full screen" : "Full screen";
    queueChartResize();
    window.setTimeout(settleFullscreenChartLayout, 160);
  }

  async function toggleFullscreen() {
    try {
      if (state.browserDemo) {
        if (document.fullscreenElement) await document.exitFullscreen();
        else await document.documentElement.requestFullscreen();
        state.fullscreen = Boolean(document.fullscreenElement);
      } else {
        const result = await window.pywebview.api.toggle_fullscreen();
        if (!result || !result.ok) return;
        state.fullscreen = Boolean(result.fullscreen);
      }
      renderFullscreenState();
    } catch (_error) {
      elements.fullscreenToggle.title = "Full screen is unavailable";
    }
  }

  function renderTimeframes() {
    elements.timeframeList.replaceChildren();
    state.timeframes.forEach((timeframe, index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `timeframe-button${timeframe === state.timeframe ? " active" : ""}`;
      button.textContent = timeframe;
      button.title = `${timeframe} chart · shortcut ${index + 1}`;
      button.setAttribute("aria-pressed", String(timeframe === state.timeframe));
      button.addEventListener("click", () => chooseTimeframe(timeframe));
      elements.timeframeList.appendChild(button);
    });
  }

  function familyRank(family) {
    const normalized = String(family || "Other");
    const index = FAMILY_ORDER.findIndex((candidate) => normalized.toLowerCase().includes(candidate.toLowerCase()));
    return index === -1 ? FAMILY_ORDER.length : index;
  }

  function normalizeGroupId(value) {
    return String(value || "other")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "_")
      .replace(/^_+|_+$/g, "") || "other";
  }

  function activeGroupingMode() {
    return state.uiPreferences.market_grouping_mode === "alpha_tier" && state.alphaTierGroupingAvailable
      ? "alpha_tier"
      : "sector";
  }

  function groupingPreferenceKeys(mode = activeGroupingMode()) {
    return mode === "alpha_tier"
      ? { order: "alpha_tier_group_order", collapsed: "collapsed_alpha_tier_groups" }
      : { order: "sector_group_order", collapsed: "collapsed_sector_groups" };
  }

  function marketGroupDetails(market, mode = activeGroupingMode()) {
    if (mode === "alpha_tier") {
      const id = normalizeGroupId(market.alpha_tier_group);
      return { id, label: ALPHA_TIER_LABELS[id] || "Alpha tier unavailable" };
    }
    const label = market.family || "Other";
    return { id: normalizeGroupId(label), label };
  }

  function normalizeGroupOrder(savedOrder, canonicalIds) {
    const known = new Set(canonicalIds);
    const ordered = [];
    (Array.isArray(savedOrder) ? savedOrder : []).forEach((groupId) => {
      const normalized = normalizeGroupId(groupId);
      if (known.has(normalized) && !ordered.includes(normalized)) ordered.push(normalized);
    });
    canonicalIds.forEach((groupId) => {
      if (!ordered.includes(groupId)) ordered.push(groupId);
    });
    return ordered;
  }

  function canonicalMarketGroups(mode = activeGroupingMode()) {
    const sorted = [...state.markets].sort((left, right) => {
      if (mode === "sector") return familyRank(left.family) - familyRank(right.family);
      const leftId = marketGroupDetails(left, mode).id;
      const rightId = marketGroupDetails(right, mode).id;
      return Object.keys(ALPHA_TIER_LABELS).indexOf(leftId) - Object.keys(ALPHA_TIER_LABELS).indexOf(rightId);
    });
    const groups = new Map();
    sorted.forEach((market) => {
      const details = marketGroupDetails(market, mode);
      if (!groups.has(details.id)) groups.set(details.id, { ...details, markets: [] });
      groups.get(details.id).markets.push(market);
    });
    return groups;
  }

  function updateGroupingControls() {
    const mode = activeGroupingMode();
    elements.groupBySector.setAttribute("aria-pressed", String(mode === "sector"));
    elements.groupByAlpha.setAttribute("aria-pressed", String(mode === "alpha_tier"));
    elements.groupByAlpha.disabled = !state.alphaTierGroupingAvailable;
    elements.groupByAlpha.title = state.alphaTierGroupingAvailable
      ? "Group each market by its earliest configured research tier"
      : "Alpha-tier grouping is unavailable; sector grouping remains active";
  }

  function announceGroupOrder(message) {
    elements.groupReorderStatus.textContent = "";
    window.setTimeout(() => { elements.groupReorderStatus.textContent = message; }, 0);
  }

  function reorderMarketGroup(groupId, targetId, placeAfter = false) {
    const mode = activeGroupingMode();
    const groups = canonicalMarketGroups(mode);
    const keys = groupingPreferenceKeys(mode);
    const canonicalIds = [...groups.keys()];
    const order = normalizeGroupOrder(state.uiPreferences[keys.order], canonicalIds);
    const fromIndex = order.indexOf(groupId);
    const targetIndexBeforeRemoval = order.indexOf(targetId);
    if (fromIndex < 0 || targetIndexBeforeRemoval < 0 || groupId === targetId) return;
    order.splice(fromIndex, 1);
    let targetIndex = order.indexOf(targetId);
    if (placeAfter) targetIndex += 1;
    order.splice(targetIndex, 0, groupId);
    state.uiPreferences[keys.order] = order;
    const label = groups.get(groupId)?.label || groupId;
    announceGroupOrder(`${label} moved to position ${order.indexOf(groupId) + 1} of ${order.length}.`);
    renderMarkets();
    persistUiPreferences();
  }

  function reorderMarketGroupByOffset(groupId, offset) {
    const mode = activeGroupingMode();
    const groups = canonicalMarketGroups(mode);
    const keys = groupingPreferenceKeys(mode);
    const order = normalizeGroupOrder(state.uiPreferences[keys.order], [...groups.keys()]);
    const currentIndex = order.indexOf(groupId);
    const nextIndex = Math.max(0, Math.min(order.length - 1, currentIndex + offset));
    if (currentIndex < 0 || nextIndex === currentIndex) return;
    const targetId = order[nextIndex];
    reorderMarketGroup(groupId, targetId, offset > 0);
    window.requestAnimationFrame(() => elements.marketList.querySelector(`[data-drag-group="${groupId}"]`)?.focus());
  }

  function toggleMarketGroup(groupId) {
    const keys = groupingPreferenceKeys();
    const collapsed = new Set(Array.isArray(state.uiPreferences[keys.collapsed]) ? state.uiPreferences[keys.collapsed] : []);
    if (collapsed.has(groupId)) collapsed.delete(groupId);
    else collapsed.add(groupId);
    state.uiPreferences[keys.collapsed] = [...collapsed];
    renderMarkets();
    persistUiPreferences();
  }

  function renderMarkets() {
    const query = elements.marketSearch.value.trim().toLowerCase();
    const exactSymbolQuery = Boolean(query) && state.markets.some(
      (market) => market.symbol.toLowerCase() === query,
    );
    const mode = activeGroupingMode();
    updateGroupingControls();
    const groups = canonicalMarketGroups(mode);
    const keys = groupingPreferenceKeys(mode);
    const groupOrder = normalizeGroupOrder(state.uiPreferences[keys.order], [...groups.keys()]);
    const collapsed = new Set(Array.isArray(state.uiPreferences[keys.collapsed]) ? state.uiPreferences[keys.collapsed] : []);
    elements.marketList.replaceChildren();
    groupOrder.forEach((groupId) => {
      const group = groups.get(groupId);
      if (!group) return;
      const groupMarkets = group.markets.filter((market) => {
        if (!query) return true;
        const symbol = market.symbol.toLowerCase();
        if (exactSymbolQuery) return symbol === query;
        return symbol.includes(query)
          || String(market.family || "").toLowerCase().includes(query)
          || group.label.toLowerCase().includes(query);
      });
      if (query && !groupMarkets.length) return;

      const section = document.createElement("section");
      section.className = "market-group";
      section.dataset.group = groupId;
      const heading = document.createElement("div");
      heading.className = "market-family";

      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "market-family-toggle";
      const expanded = Boolean(query) || !collapsed.has(groupId);
      toggle.setAttribute("aria-expanded", String(expanded));
      toggle.setAttribute("aria-label", `${expanded ? "Collapse" : "Expand"} ${group.label}, ${groupMarkets.length} markets`);
      const chevron = document.createElement("span");
      chevron.className = "chevron";
      chevron.textContent = "▾";
      const label = document.createElement("span");
      label.className = "market-family-label";
      label.textContent = group.label;
      const count = document.createElement("span");
      count.className = "market-family-count";
      count.textContent = String(groupMarkets.length);
      toggle.append(chevron, label, count);
      toggle.addEventListener("click", () => toggleMarketGroup(groupId));

      const drag = document.createElement("button");
      drag.type = "button";
      drag.className = "market-group-drag";
      drag.textContent = "⠿";
      drag.draggable = !query;
      drag.disabled = Boolean(query);
      drag.dataset.dragGroup = groupId;
      drag.setAttribute("aria-label", `Reorder ${group.label}`);
      drag.setAttribute("aria-keyshortcuts", "Alt+ArrowUp Alt+ArrowDown");
      drag.title = query ? "Clear search to reorder groups" : "Drag, or press Alt+Up/Down";
      drag.addEventListener("keydown", (event) => {
        if (!event.altKey || !["ArrowUp", "ArrowDown"].includes(event.key)) return;
        event.preventDefault();
        event.stopPropagation();
        reorderMarketGroupByOffset(groupId, event.key === "ArrowUp" ? -1 : 1);
      });
      drag.addEventListener("dragstart", (event) => {
        state.draggedMarketGroup = groupId;
        section.classList.add("dragging");
        event.dataTransfer.effectAllowed = "move";
        event.dataTransfer.setData("text/plain", groupId);
      });
      drag.addEventListener("dragend", () => {
        state.draggedMarketGroup = null;
        section.classList.remove("dragging");
        elements.marketList.querySelectorAll(".drag-over").forEach((element) => element.classList.remove("drag-over"));
      });
      section.addEventListener("dragover", (event) => {
        if (query || !state.draggedMarketGroup || state.draggedMarketGroup === groupId) return;
        event.preventDefault();
        section.classList.add("drag-over");
      });
      section.addEventListener("dragleave", () => section.classList.remove("drag-over"));
      section.addEventListener("drop", (event) => {
        event.preventDefault();
        section.classList.remove("drag-over");
        const dragged = state.draggedMarketGroup || event.dataTransfer.getData("text/plain");
        const bounds = section.getBoundingClientRect();
        reorderMarketGroup(dragged, groupId, event.clientY > bounds.top + bounds.height / 2);
      });
      heading.append(toggle, drag);
      section.appendChild(heading);

      if (expanded) groupMarkets.forEach((market) => {
        const status = state.statuses.get(market.symbol) || market;
        const button = document.createElement("button");
        button.type = "button";
        button.className = `market-row${market.symbol === state.selectedMarket ? " selected" : ""}`;
        button.dataset.market = market.symbol;
        button.setAttribute("aria-label", `${market.symbol} ${status.state || status.status || "waiting"}`);

        const name = document.createElement("span");
        name.className = "market-name";
        const dot = document.createElement("span");
        dot.className = `status-dot ${statusClass(status.state || status.status)}`;
        const symbol = document.createElement("strong");
        symbol.textContent = market.symbol;
        name.append(dot, symbol);

        const last = document.createElement("span");
        last.className = "market-value";
        last.textContent = formatPrice(status.last);

        const change = document.createElement("span");
        const numericChange = status.change_1m === null || status.change_1m === undefined ? null : Number(status.change_1m);
        const direction = numericChange === null ? "flat" : numericChange > 0 ? "up" : numericChange < 0 ? "down" : "flat";
        change.className = `market-change ${direction}`;
        change.textContent = numericChange === null ? "—" : `${numericChange >= 0 ? "+" : ""}${numericChange.toFixed(2)}%`;

        button.append(name, last, change);
        button.addEventListener("click", () => chooseMarket(market.symbol));
        section.appendChild(button);
      });
      elements.marketList.appendChild(section);
    });
    elements.marketCount.textContent = `${state.markets.length} markets`;
  }

  function sourceLabel(source) {
    const normalized = String(source || "waiting").toLowerCase();
    const labels = {
      waiting: "Waiting for chart data",
      switching: "Switching market",
      "contract-resolved": "Live feed ready",
      cached: "Cached bars",
      "selection-cache": "Cached bars",
      historical: "History + live",
      "recent-replay": "History + live",
      "live-only": "Live bars",
      "timeframe-cache": "Cached + live",
      demo: "Demo history",
      "browser demo": "Demo history",
    };
    if (normalized.startsWith("loading")) return "Loading selected interval";
    return labels[normalized] || "Chart data";
  }

  function renderSourceState() {
    const label = sourceLabel(state.source);
    const barWord = state.barCount === 1 ? "bar" : "bars";
    const range = state.barCount > 0 ? ` · ${loadedChartRange()}` : "";
    const base = state.source === "waiting"
      ? label
      : `${label} · ${state.barCount.toLocaleString()} ${state.timeframe} ${barWord}${range}`;
    elements.sourceState.className = "source-state";
    elements.sourceState.title = "";
    elements.retryHistory.hidden = true;
    elements.retryHistory.disabled = false;
    elements.retryHistory.textContent = "Retry history";
    const cacheState = String(state.historyCache?.state || "CHECKING").toUpperCase();
    if (cacheState === "WARMING" || cacheState === "PAUSED") {
      elements.sourceState.textContent = `${base} · ${cacheState === "PAUSED" ? "Cache update paused" : "Cache updating"}`;
      elements.sourceState.classList.add("history-loading");
      elements.sourceState.title = elements.sourceState.textContent;
      return;
    }
    if (cacheState === "CONFIRMATION_REQUIRED") {
      elements.sourceState.textContent = `${base} · Missing recent history—approval required`;
      elements.retryHistory.hidden = false;
      elements.retryHistory.textContent = "Review & approve update";
      elements.sourceState.title = elements.sourceState.textContent;
      return;
    }
    if (cacheState === "ERROR" || cacheState === "PARTIAL") {
      elements.sourceState.textContent = `${base} · History incomplete`;
      elements.sourceState.classList.add("history-error");
      elements.retryHistory.hidden = false;
      elements.sourceState.title = elements.sourceState.textContent;
      return;
    }
    if (state.historyState === "BACKFILLING") {
      elements.sourceState.textContent = `${base} · Loading history…`;
      elements.sourceState.classList.add("history-loading");
      elements.retryHistory.hidden = false;
      elements.retryHistory.disabled = true;
      elements.retryHistory.textContent = "Loading…";
      return;
    }
    if (state.historyState === "ERROR") {
      const failureLabels = {
        TIMEOUT: "timeout",
        CONNECTION: "connection",
        DATA_AVAILABILITY: "provider delay",
      };
      const detail = failureLabels[state.historyCategory];
      elements.sourceState.textContent = `${base} · History unavailable${detail ? ` (${detail})` : ""}`;
      elements.sourceState.classList.add("history-error");
      elements.sourceState.title = state.historyMessage;
      elements.retryHistory.hidden = false;
      return;
    }
    elements.sourceState.textContent = base;
    elements.sourceState.title = base;
  }

  function resetHistoryState() {
    state.source = "waiting";
    state.barCount = 0;
    state.historyState = "IDLE";
    state.historyMessage = "";
    state.historyCategory = "";
    renderSourceState();
  }

  function applyHistoryStatus(payload) {
    if (payload.market && payload.market !== state.selectedMarket) return;
    const incomingGeneration = Number(payload.generation || 0);
    if (incomingGeneration && incomingGeneration < state.generation) return;
    if (incomingGeneration) state.generation = Math.max(state.generation, incomingGeneration);
    state.historyState = String(payload.state || "IDLE").toUpperCase();
    state.historyMessage = String(payload.message || "");
    state.historyCategory = state.historyState === "ERROR"
      ? String(payload.failure_category || "").toUpperCase()
      : "";
    renderSourceState();
  }

  function setHistoryCachePopover(open) {
    elements.historyCachePopover.hidden = !open;
    elements.historyCacheToggle.setAttribute("aria-expanded", String(open));
    if (open) elements.historyCacheClose.focus({ preventScroll: true });
  }

  function formatHistoryCost(value) {
    if (value === null || value === undefined || value === "") return "—";
    const cost = Number(value);
    if (!Number.isFinite(cost)) return "—";
    if (cost > 0 && cost < 0.0001) return "<$0.0001";
    return `$${cost.toFixed(4)}`;
  }

  function renderHistoryCache() {
    const cache = state.historyCache || {};
    const cacheState = String(cache.state || "CHECKING").toUpperCase();
    const ready = Math.max(0, Number(cache.ready_markets || 0));
    const total = Math.max(1, Number(cache.total_markets || state.markets.length || 33));
    const queued = Math.max(0, Number(cache.queued_markets || 0));
    const titles = {
      CHECKING: "Checking coverage",
      CONFIRMATION_REQUIRED: "Update available",
      WARMING: "Updating history",
      PAUSED: "Update paused",
      COMPLETE: "History ready",
      PARTIAL: "History incomplete",
      ERROR: "History update unavailable",
    };
    elements.historyCacheCount.textContent = `${ready}/${total}`;
    elements.historyCacheReady.textContent = `${ready} of ${total}`;
    elements.historyCacheQueued.textContent = `${queued} ${queued === 1 ? "market" : "markets"}`;
    elements.historyCacheTitle.textContent = titles[cacheState] || "History cache";
    elements.historyCacheMessage.textContent = cacheState === "CONFIRMATION_REQUIRED"
      ? `${cache.message || "A history update is available."} No download has started. The chart remains on ${loadedChartRange()} until you approve the displayed estimate.`
      : cache.message || "History cache status unavailable.";
    elements.historyCacheCost.textContent = formatHistoryCost(cache.estimated_cost_usd);
    elements.historyCacheExpiry.textContent = Number.isFinite(Number(cache.estimate_expires_at))
      ? new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" }).format(new Date(Number(cache.estimate_expires_at) * 1000))
      : "—";
    setDot(elements.historyCacheDot, cacheState);
    elements.historyCacheConfirm.hidden = cacheState !== "CONFIRMATION_REQUIRED";
    elements.historyCacheConfirm.textContent = `Approve estimate & update ${total} markets`;
    elements.historyCachePause.hidden = !["WARMING", "PAUSED"].includes(cacheState);
    elements.historyCachePause.textContent = cacheState === "PAUSED" ? "Resume" : "Pause";
    elements.historyCacheRetry.hidden = !["ERROR", "PARTIAL"].includes(cacheState);
    elements.historyCacheToggle.title = cache.message || titles[cacheState] || "History cache";
    renderSourceState();
    renderDataHealth();
  }

  function applyHistoryCacheStatus(payload) {
    const previousPlan = state.historyCache?.plan_id;
    state.historyCache = { ...state.historyCache, ...payload };
    renderHistoryCache();
    if (
      payload.state === "CONFIRMATION_REQUIRED" &&
      payload.plan_id &&
      payload.plan_id !== previousPlan &&
      payload.plan_id !== state.historyPopoverDismissedPlanId
    ) {
      setHistoryCachePopover(true);
    }
  }

  async function confirmHistoryCache() {
    const planId = state.historyCache?.plan_id;
    if (state.browserDemo || !planId) return;
    elements.historyCacheConfirm.disabled = true;
    try {
      const result = await window.pywebview.api.confirm_history_cache(planId);
      if (!result?.ok) throw new Error("confirmation was not accepted");
    } catch (_error) {
      state.historyCache = {
        ...state.historyCache,
        state: "ERROR",
        message: "The estimate expired or changed. Refresh it before downloading.",
      };
      renderHistoryCache();
    } finally {
      elements.historyCacheConfirm.disabled = false;
    }
  }

  async function toggleHistoryCachePause() {
    if (state.browserDemo) return;
    const paused = String(state.historyCache?.state).toUpperCase() !== "PAUSED";
    const result = await window.pywebview.api.set_history_cache_paused(paused);
    if (!result?.ok) renderHistoryCache();
  }

  async function refreshHistoryCacheEstimate() {
    if (state.browserDemo) return;
    elements.historyCacheRetry.disabled = true;
    try {
      const result = await window.pywebview.api.retry_history_cache_estimate();
      if (!result?.ok) throw new Error("estimate refresh was not accepted");
    } finally {
      elements.historyCacheRetry.disabled = false;
    }
  }

  async function chooseMarket(market) {
    if (!market || market === state.selectedMarket) return;
    const previousMarket = state.selectedMarket;
    const previousContract = state.contract;
    const previousGeneration = state.generation;
    const hasVisibleChart = state.barCount > 0;
    const previousHistory = {
      source: state.source,
      barCount: state.barCount,
      latestBar: state.latestBar,
      historyState: state.historyState,
      historyMessage: state.historyMessage,
      historyCategory: state.historyCategory,
    };
    state.selectedMarket = market;
    state.generation += 1;
    elements.instrumentSymbol.textContent = market;
    elements.instrumentContract.textContent = hasVisibleChart
      ? `Loading ${market} · showing ${previousContract || previousMarket}`
      : "Resolving contract";
    elements.chartEmpty.classList.toggle("hidden", hasVisibleChart);
    if (!hasVisibleChart) {
      clearSelectionContext();
      elements.chartEmptyDetail.textContent = `Switching the focus stream to ${market}.`;
      resetHistoryState();
      state.barCount = 0;
      state.earliestBar = null;
      state.latestBar = null;
      updateQuote(null);
    }
    state.source = "switching";
    renderSourceState();
    renderMarkets();
    setFocusStatus("RESOLVING", `Resolving ${market}`);
    if (state.browserDemo) {
      browserDemoSnapshot(market, state.timeframe);
      return;
    }
    try {
      const result = await window.pywebview.api.select_market(market);
      if (!result || !result.ok) throw new Error("market was not accepted");
      state.generation = Math.max(state.generation, Number(result.generation || 0));
    } catch (error) {
      state.selectedMarket = previousMarket;
      state.contract = previousContract;
      state.generation = previousGeneration;
      Object.assign(state, previousHistory);
      elements.instrumentSymbol.textContent = previousMarket;
      elements.instrumentContract.textContent = previousContract || previousMarket;
      elements.chartEmpty.classList.toggle("hidden", state.barCount > 0);
      renderSourceState();
      updateQuote(state.latestBar);
      renderMarkets();
      setFocusStatus("ERROR", String(error));
    }
  }

  async function chooseTimeframe(timeframe) {
    if (!state.timeframes.includes(timeframe) || timeframe === state.timeframe) return;
    state.timeframe = timeframe;
    clearSelectionContext();
    renderTimeframes();
    state.source = "loading cached timeframe";
    state.barCount = 0;
    renderSourceState();
    if (state.browserDemo) {
      browserDemoSnapshot(state.selectedMarket, timeframe);
      return;
    }
    try {
      const result = await window.pywebview.api.select_timeframe(timeframe);
      if (!result || !result.ok) throw new Error("timeframe was not accepted");
    } catch (error) {
      setFocusStatus("ERROR", String(error));
    }
  }

  async function retrySelectedMarket() {
    if (state.browserDemo) {
      browserDemoSnapshot(state.selectedMarket, state.timeframe);
      return;
    }
    elements.chartEmpty.classList.remove("error");
    setFocusStatus("RESOLVING", `Retrying ${state.selectedMarket} contract lookup`);
    try {
      const result = await window.pywebview.api.select_market(state.selectedMarket);
      if (!result || !result.ok) throw new Error("market retry was not accepted");
      state.generation = Math.max(state.generation, Number(result.generation || 0));
    } catch (error) {
      setFocusStatus("ERROR", String(error));
    }
  }

  async function retryHistory() {
    if (state.browserDemo) return;
    setHistoryCachePopover(true);
    if (String(state.historyCache?.state).toUpperCase() === "CONFIRMATION_REQUIRED") return;
    await refreshHistoryCacheEstimate();
  }

  function setFocusStatus(feedState, message) {
    const normalized = String(feedState || "WAITING").toUpperCase();
    elements.focusState.textContent = normalized.replaceAll("_", " ");
    elements.focusState.className = `state-badge ${statusClass(normalized)}`;
    elements.footerStatus.textContent = message || normalized;
    elements.retryFocus.hidden = normalized !== "ERROR";
    setDot(elements.footerDot, normalized);
    const emptyTitles = {
      RESOLVING: "Resolving contract",
      BACKFILLING: "Loading market history",
      CONNECTING: "Connecting live feed",
      ERROR: "Market view unavailable",
      HISTORICAL_ONLY: "Showing cached history",
    };
    if (emptyTitles[normalized]) {
      elements.chartEmptyTitle.textContent = emptyTitles[normalized];
      elements.chartEmptyDetail.textContent = message || normalized;
      elements.chartEmpty.classList.toggle("error", normalized === "ERROR");
    }
    if (normalized === "ERROR" && state.startupWatchdog) {
      window.clearTimeout(state.startupWatchdog);
      state.startupWatchdog = null;
    }
  }

  function setOverviewStatus(feedState, message) {
    elements.overviewLabel.textContent = message || `Overview ${String(feedState).toLowerCase()}`;
    setDot(elements.overviewDot, feedState);
  }

  function applyBootstrap(payload) {
    state.markets = Array.isArray(payload.markets) ? payload.markets : [];
    state.selectedMarket = payload.selected_market || "ES";
    state.timeframe = payload.timeframe || "1m";
    state.timeframes = Array.isArray(payload.timeframes) ? payload.timeframes : Object.keys(TIMEFRAME_SECONDS);
    state.mode = payload.mode || "live";
    state.executionCapability = payload.execution_capability || state.executionCapability;
    state.predictionCapability = payload.prediction_capability || {
      mode: "offline",
      synthetic: false,
      observation_only: true,
    };
    const groupingCapability = payload.market_grouping_capability || {};
    state.alphaTierGroupingAvailable = groupingCapability.alpha_tiers_available === true;
    state.alphaTierGroups = Array.isArray(groupingCapability.alpha_tier_groups)
      ? groupingCapability.alpha_tier_groups
      : [];
    state.contract = "";
    state.statuses.clear();
    state.markets.forEach((market) => state.statuses.set(market.symbol, { ...market, state: market.status || "WAITING" }));
    elements.instrumentSymbol.textContent = state.selectedMarket;
    elements.instrumentMeta.textContent = "Continuous front contract · Computer local time";
    resetHistoryState();
    applyUiPreferences(payload.ui_preferences || {});
    clearSelectionContext();
    renderTimeframes();
    renderExecution();
    renderMarkets();
    initializeChart();
    if (state.startupWatchdog) window.clearTimeout(state.startupWatchdog);
    state.startupWatchdog = window.setTimeout(() => {
      state.startupWatchdog = null;
      if (!elements.chartEmpty.classList.contains("hidden")) {
        setFocusStatus(
          "ERROR",
          "Market-data startup exceeded 60 seconds. Close and retry; the provider did not respond in time.",
        );
      }
    }, 60000);
  }

  function applySnapshot(payload) {
    if (payload.market !== state.selectedMarket) return;
    if (payload.timeframe !== state.timeframe) return;
    const incomingGeneration = Number(payload.generation || 0);
    if (incomingGeneration < state.generation) return;
    const identityChanged = incomingGeneration !== state.generation || payload.contract !== state.contract;
    state.generation = incomingGeneration;
    state.contract = payload.contract || payload.market;
    if (identityChanged) clearSelectionContext();
    elements.instrumentContract.textContent = payload.contract || payload.market;
    const bars = Array.isArray(payload.bars) ? payload.bars : [];
    state.barCloses = new Map(
      bars.map((bar) => [Number(bar.time), Number(bar.close)]),
    );
    initializeChart();
    if (!state.candleSeries || !state.volumeSeries) {
      elements.chartEmptyDetail.textContent = "The bundled chart library did not load.";
      setFocusStatus("ERROR", "Chart runtime unavailable");
      return;
    }
    const candleData = bars.map(({ time, open, high, low, close }) => ({ time, open, high, low, close }));
    const volumeData = bars.map((bar) => ({
      time: bar.time,
      value: bar.volume,
      color: bar.close >= bar.open ? "rgba(53, 199, 160, 0.34)" : "rgba(240, 111, 121, 0.32)",
    }));
    state.candleSeries.setData(candleData);
    state.volumeSeries.setData(volumeData);
    state.sessionMarkers = Array.isArray(payload.markers) ? payload.markers : [];
    queueSessionBoundaryRender();
    state.latestBar = bars.at(-1) || null;
    state.earliestBar = bars[0] || null;
    updateQuote(state.latestBar);
    state.source = payload.source || "unknown";
    state.barCount = bars.length;
    renderSourceState();
    if (payload.source !== "contract-resolved" && state.startupWatchdog) {
      window.clearTimeout(state.startupWatchdog);
      state.startupWatchdog = null;
    }
    elements.chartEmpty.classList.remove("error");
    elements.chartEmpty.classList.toggle("hidden", bars.length > 0);
    if (!bars.length) {
      elements.chartEmptyTitle.textContent = payload.source === "contract-resolved"
        ? "Contract resolved"
        : "Waiting for the first live trade";
      elements.chartEmptyDetail.textContent = payload.source === "contract-resolved"
        ? "The exact contract is ready; bounded history is loading."
        : "The feed is connected and the chart will update when a trade arrives.";
    }
    if (bars.length) scheduleChartReset();
    renderTimeframes();
  }

  function matchesFocusedIdentity(payload) {
    return (
      payload?.market === state.selectedMarket &&
      payload?.timeframe === state.timeframe &&
      payload?.contract === state.contract &&
      Number(payload?.generation) === state.generation
    );
  }

  function applyPrediction(payload) {
    if (!matchesFocusedIdentity(payload)) return;
    state.prediction = payload;
    renderPrediction();
  }

  function applyDataHealth(payload) {
    if (!matchesFocusedIdentity(payload)) return;
    state.dataHealth = payload;
    renderDataHealth();
  }

  function applyBarUpdate(payload) {
    if (
      payload.market !== state.selectedMarket ||
      payload.timeframe !== state.timeframe ||
      Number(payload.generation) !== state.generation ||
      !payload.bar
    ) return;
    const bar = payload.bar;
    const isNewBar = !state.latestBar || Number(bar.time) > Number(state.latestBar.time);
    const becameLiveOnly = state.source === "contract-resolved" || state.source === "switching";
    if (becameLiveOnly) state.source = "live-only";
    state.candleSeries.update({ time: bar.time, open: bar.open, high: bar.high, low: bar.low, close: bar.close });
    state.volumeSeries.update({
      time: bar.time,
      value: bar.volume,
      color: bar.close >= bar.open ? "rgba(53, 199, 160, 0.34)" : "rgba(240, 111, 121, 0.32)",
    });
    state.latestBar = bar;
    if (!state.earliestBar) state.earliestBar = bar;
    state.barCloses.set(Number(bar.time), Number(bar.close));
    if (isNewBar) state.barCount += 1;
    if (state.dataHealth) {
      state.dataHealth.last_bar_time = Number(bar.time);
      if (state.dataHealth.history) state.dataHealth.history.bar_count = state.barCount;
      renderDataHealth();
    }
    if (isNewBar || becameLiveOnly) renderSourceState();
    elements.chartEmpty.classList.add("hidden");
    elements.chartEmpty.classList.remove("error");
    if (state.startupWatchdog) {
      window.clearTimeout(state.startupWatchdog);
      state.startupWatchdog = null;
    }
    updateQuote(bar);
    if (becameLiveOnly) scheduleChartReset();
    else if (isNewBar) followLatestBar();
  }

  function applyMarketStatus(payload) {
    if (!payload.market) return;
    const current = state.statuses.get(payload.market) || { symbol: payload.market };
    state.statuses.set(payload.market, { ...current, ...payload });
    renderMarkets();
  }

  function applyFeedStatus(payload) {
    if (payload.scope === "overview") setOverviewStatus(payload.state, payload.message);
    else if (payload.scope === "history") applyHistoryStatus(payload);
    else if (payload.scope === "focus") setFocusStatus(payload.state, payload.message);
    else {
      setOverviewStatus(payload.state, payload.message);
      setFocusStatus(payload.state, payload.message);
    }
  }

  function receive(message) {
    if (!message || message.v !== PROTOCOL_VERSION || !message.payload) return;
    if (message.type === "bootstrap") applyBootstrap(message.payload);
    else if (message.type === "chart_snapshot") applySnapshot(message.payload);
    else if (message.type === "bar_update") applyBarUpdate(message.payload);
    else if (message.type === "market_status") applyMarketStatus(message.payload);
    else if (message.type === "feed_status") applyFeedStatus(message.payload);
    else if (message.type === "data_health") applyDataHealth(message.payload);
    else if (message.type === "prediction_update") applyPrediction(message.payload);
    else if (message.type === "history_cache_status") applyHistoryCacheStatus(message.payload);
    else if (message.type === "execution_capability" || message.type === "execution_readiness") {
      state.executionCapability = message.payload;
      renderExecution();
    }
  }

  function newestCurrentBarEventAge(messages, nowMs) {
    const ages = (Array.isArray(messages) ? messages : [])
      .filter((message) => (
        message?.type === "bar_update"
        && message.payload?.market === state.selectedMarket
        && message.payload?.timeframe === state.timeframe
        && Number(message.payload?.generation || 0) >= state.generation
      ))
      .map((message) => Date.parse(message.sent_at))
      .filter(Number.isFinite)
      .map((sentAt) => Math.max(0, nowMs - sentAt));
    return ages.length ? Math.min(...ages) : null;
  }

  function observePollHealth(messages, pollDurationMs, nowMs = Date.now()) {
    if (document.hidden) {
      state.unhealthyPolls = 0;
      state.healthySince = null;
      syncVisualUpdateActivity();
      return { constrained: state.visualUpdateConstrained, effective_hz: effectiveVisualUpdateHz() };
    }
    const eventAge = newestCurrentBarEventAge(messages, nowMs);
    const batchSize = Array.isArray(messages) ? messages.length : 0;
    const unhealthy = (eventAge !== null && eventAge > 500)
      || pollDurationMs > 250
      || batchSize >= POLL_EVENT_LIMIT;
    if (unhealthy) {
      state.unhealthyPolls += 1;
      state.healthySince = null;
      if (state.unhealthyPolls >= UNHEALTHY_POLL_LIMIT) state.visualUpdateConstrained = true;
    } else {
      state.unhealthyPolls = 0;
      const healthy = (eventAge === null || eventAge < 250)
        && pollDurationMs < 150
        && batchSize < 50;
      if (state.visualUpdateConstrained && healthy) {
        if (state.healthySince === null) state.healthySince = nowMs;
        if (nowMs - state.healthySince >= RECOVERY_WINDOW_MS) {
          state.visualUpdateConstrained = false;
          state.healthySince = null;
        }
      } else if (!healthy) {
        state.healthySince = null;
      }
    }
    syncVisualUpdateActivity();
    return { constrained: state.visualUpdateConstrained, effective_hz: effectiveVisualUpdateHz() };
  }

  window.cockpit = { receive, observePollHealth };

  async function pollDesktopEvents() {
    if (!state.bridgeReady || state.browserDemo || state.pollInFlight) return;
    const pollStarted = window.performance.now();
    let messages = [];
    state.pollInFlight = true;
    try {
      messages = await window.pywebview.api.poll_events(POLL_EVENT_LIMIT);
      if (Array.isArray(messages)) messages.forEach(receive);
      else messages = [];
    } catch (error) {
      setFocusStatus("ERROR", `Desktop event bridge unavailable: ${error}`);
    } finally {
      const elapsed = window.performance.now() - pollStarted;
      observePollHealth(messages, elapsed);
      state.pollInFlight = false;
      if (state.bridgeReady && !state.browserDemo) {
        const interval = 1000 / effectiveVisualUpdateHz();
        state.pollTimer = window.setTimeout(pollDesktopEvents, Math.max(0, interval - elapsed));
      }
    }
  }

  const DEMO_MARKETS = [
    ["ES", "Equity Index"], ["NQ", "Equity Index"], ["RTY", "Equity Index"], ["YM", "Equity Index"],
    ["CL", "Energy"], ["NG", "Energy"], ["RB", "Energy"], ["HO", "Energy"],
    ["GC", "Metals"], ["SI", "Metals"], ["HG", "Metals"],
    ["SR3", "Rates"], ["SR1", "Rates"], ["TN", "Rates"], ["ZT", "Rates"], ["ZF", "Rates"], ["ZN", "Rates"], ["ZB", "Rates"], ["UB", "Rates"],
    ["6A", "FX"], ["6B", "FX"], ["6C", "FX"], ["6E", "FX"], ["6J", "FX"], ["6M", "FX"],
    ["ZC", "Agriculture"], ["ZS", "Agriculture"], ["ZL", "Agriculture"], ["ZM", "Agriculture"], ["ZW", "Agriculture"], ["KE", "Agriculture"],
    ["LE", "Livestock"], ["HE", "Livestock"],
  ];
  const DEMO_ALPHA_TIER_GROUPS = {
    tier_1_core: ["ES", "CL", "ZN", "6E"],
    tier_2_additions: ["NQ", "NG", "GC", "HG", "SR3", "ZB", "6J", "6B", "ZC", "ZS", "LE"],
    tier_3_additions: ["RTY", "YM", "RB", "HO", "SI", "SR1", "TN", "ZT", "ZF", "UB", "6A", "6C", "6M", "ZL", "ZM", "ZW", "KE", "HE"],
  };

  function demoAlphaTierGroup(symbol) {
    return Object.keys(DEMO_ALPHA_TIER_GROUPS).find((groupId) => DEMO_ALPHA_TIER_GROUPS[groupId].includes(symbol)) || null;
  }

  function demoBars(market) {
    if (state.browserBars.has(market)) return state.browserBars.get(market);
    const marketIndex = DEMO_MARKETS.findIndex(([symbol]) => symbol === market);
    const anchors = { ES: 6378.25, NQ: 23214.5, CL: 68.42, GC: 3368.2 };
    const base = anchors[market] || 85 + marketIndex * 17.25;
    const seed = [...market].reduce((sum, character) => sum + character.charCodeAt(0), 0);
    const end = Math.floor(Date.now() / 60000) * 60;
    const start = end - 3 * 24 * 60 * 60;
    const bars = [];
    let previous = base;
    for (let index = 0; index < 3 * 24 * 60; index += 1) {
      const wave = Math.sin((index + seed) / 31) * base * 0.00055;
      const drift = Math.sin((index + seed) / 257) * base * 0.00008;
      const close = previous + wave * 0.09 + drift;
      const spread = Math.max(base * 0.00008, Math.abs(wave) * 0.22);
      bars.push({
        time: start + index * 60,
        open: previous,
        high: Math.max(previous, close) + spread,
        low: Math.min(previous, close) - spread,
        close,
        volume: 80 + ((index * 37 + seed) % 1750),
      });
      previous = close;
    }
    state.browserBars.set(market, bars);
    return bars;
  }

  function aggregateDemo(bars, timeframe) {
    const seconds = TIMEFRAME_SECONDS[timeframe] || 60;
    if (seconds === 60) return bars;
    const result = [];
    let current = null;
    bars.forEach((bar) => {
      const bucket = Math.floor(bar.time / seconds) * seconds;
      if (!current || current.time !== bucket) {
        current = { ...bar, time: bucket };
        result.push(current);
      } else {
        current.high = Math.max(current.high, bar.high);
        current.low = Math.min(current.low, bar.low);
        current.close = bar.close;
        current.volume += bar.volume;
      }
    });
    return result;
  }

  function directionEntropy(probabilities) {
    const values = [probabilities.long, probabilities.flat, probabilities.short];
    return -values.reduce((total, value) => total + (value > 0 ? value * Math.log(value) : 0), 0) / Math.log(3);
  }

  function browserDemoPrediction(market, contract, instrumentId, timeframe, bars) {
    const predictionTime = Math.floor(Date.now() / 1000);
    const inputBarTime = bars.at(-2)?.time || null;
    const ready = {
      ES: { direction: "LONG", probabilities: { long: 0.64, flat: 0.21, short: 0.15 }, expected_return: 0.00045 },
      NQ: { direction: "SHORT", probabilities: { long: 0.20, flat: 0.18, short: 0.62 }, expected_return: -0.00055 },
    }[market];
    const nonReady = {
      RTY: ["ABSTAIN", "MODEL_ABSTAINED"],
      YM: ["WARMING_UP", "FEATURE_WARMUP_INCOMPLETE"],
      CL: ["STALE", "DATA_STALE"],
      NG: ["ERROR", "SYNTHETIC_DEMO_ERROR"],
    };
    const [predictionState, reason] = ready
      ? ["READY", null]
      : (nonReady[market] || ["ABSTAIN", "OUTSIDE_DEMO_SCENARIO"]);
    const forecast = ready && inputBarTime ? {
      direction: ready.direction,
      horizon_seconds: 900,
      probabilities: ready.probabilities,
      expected_return: ready.expected_return,
      direction_entropy: directionEntropy(ready.probabilities),
    } : null;
    const resolvedState = forecast ? predictionState : ready ? "WARMING_UP" : predictionState;
    const reasonCodes = forecast ? [] : [reason || "FEATURE_WARMUP_INCOMPLETE"];
    return {
      market,
      contract,
      instrument_id: instrumentId,
      timeframe,
      generation: state.generation,
      prediction_id: `synthetic_demo:${market}:${timeframe}:${inputBarTime || predictionTime}:${resolvedState.toLowerCase()}`,
      prediction_time: predictionTime,
      input_bar_time: inputBarTime,
      state: resolvedState,
      source: "SYNTHETIC_DEMO",
      synthetic: true,
      observation_only: true,
      model: { id: "synthetic-shadow-demo", version: "1", strategy: "direction-probability-ui" },
      forecast,
      reason_codes: reasonCodes,
    };
  }

  function browserDemoHealth(market, contract, instrumentId, timeframe, bars) {
    let healthState = "CURRENT";
    let historyState = "COMPLETE";
    let continuityState = "PASS";
    let unexpectedGapCount = 0;
    let largestGapSeconds = 0;
    let reasonCodes = [];
    if (market === "RTY") {
      healthState = "DEGRADED";
      historyState = "PARTIAL";
      reasonCodes = ["HISTORY_PARTIAL"];
    } else if (market === "YM") {
      healthState = "DEGRADED";
      continuityState = "WARN";
      unexpectedGapCount = 2;
      largestGapSeconds = 180;
      reasonCodes = ["CONTINUITY_WARNING"];
    } else if (market === "CL") {
      healthState = "STALE";
      reasonCodes = ["DATA_STALE"];
    } else if (market === "NG") {
      healthState = "UNKNOWN";
      historyState = "UNAVAILABLE";
      continuityState = "NOT_EVALUATED";
      unexpectedGapCount = null;
      largestGapSeconds = null;
      reasonCodes = ["HISTORY_UNAVAILABLE", "CONTINUITY_NOT_EVALUATED"];
    }
    const coverage = bars.length > 1 ? (bars.at(-1).time - bars[0].time) / 3600 : 0;
    return {
      market,
      contract,
      instrument_id: instrumentId,
      timeframe,
      generation: state.generation,
      evaluated_at: Math.floor(Date.now() / 1000),
      last_bar_time: bars.at(-1)?.time || null,
      state: healthState,
      history: { state: historyState, requested_hours: 72, coverage_hours: coverage, bar_count: bars.length },
      continuity: { state: continuityState, unexpected_gap_count: unexpectedGapCount, largest_gap_seconds: largestGapSeconds },
      reason_codes: reasonCodes,
    };
  }

  function browserDemoSnapshot(market, timeframe) {
    state.generation += 1;
    const bars = aggregateDemo(demoBars(market), timeframe);
    const contract = `${market}M6`;
    const instrumentId = 100_000 + Math.max(0, DEMO_MARKETS.findIndex(([symbol]) => symbol === market));
    receive({
      v: PROTOCOL_VERSION,
      type: "chart_snapshot",
      payload: { market, contract, timeframe, bars, markers: [], source: "browser demo", generation: state.generation },
    });
    receive({ v: PROTOCOL_VERSION, type: "data_health", payload: browserDemoHealth(market, contract, instrumentId, timeframe, bars) });
    receive({ v: PROTOCOL_VERSION, type: "prediction_update", payload: browserDemoPrediction(market, contract, instrumentId, timeframe, bars) });
    setFocusStatus("LIVE", "Deterministic demo stream active");
  }

  function startBrowserDemo() {
    if (state.bridgeReady) return;
    state.browserDemo = true;
    const markets = DEMO_MARKETS.map(([symbol, family], index) => {
      const bars = demoBars(symbol);
      const latest = bars.at(-1);
      const previous = bars.at(-2);
      const status = index === 10 ? "STALE" : index === 20 ? "WAITING" : index === 30 ? "ERROR" : "LIVE";
      return {
        symbol,
        family,
        alpha_tier_group: demoAlphaTierGroup(symbol),
        status,
        last: latest.close,
        change_1m: (latest.close / previous.close - 1) * 100,
      };
    });
    receive({
      v: PROTOCOL_VERSION,
      type: "bootstrap",
      payload: {
        markets,
        selected_market: "ES",
        timeframe: "1m",
        timeframes: Object.keys(TIMEFRAME_SECONDS),
        display_tz: "local",
        mode: "demo",
        prediction_capability: { mode: "synthetic_demo", synthetic: true, observation_only: true },
        market_grouping_capability: {
          alpha_tiers_available: true,
          alpha_tier_groups: Object.entries(DEMO_ALPHA_TIER_GROUPS).map(([id, groupMarkets]) => ({
            id,
            label: ALPHA_TIER_LABELS[id],
            market_count: groupMarkets.length,
          })),
        },
        visual_update_capability: {
          default_mode: DEFAULT_VISUAL_UPDATE_MODE,
          modes: VISUAL_UPDATE_HZ,
          adaptive_floor_hz: VISUAL_UPDATE_HZ.efficient,
        },
        ui_preferences: {},
        execution_capability: {
          mode: "LOCAL_EXECUTION_SIMULATOR", origin: "LOCAL_SIMULATOR", simulated: true,
          provider_id: "LOCAL_EXECUTION_SIMULATOR", platform_id: "LOCAL_FAKE_BROKER", profile_id: "mff_rapid_eod_50k_2026_08_10",
          account_stage: "sim_funded", connection_id: "LOCAL-SIMULATOR", connection_hash: "0".repeat(64),
          entitlement_status: "NOT_APPLICABLE_LOCAL_SIMULATOR", account_binding_present: false, account_binding_id: null,
          execution_capability: "MANUAL_ONLY", direct_api_read_access: false, direct_api_order_access: false,
          manual_ticket_preview_available: true, manual_assistant_readiness: false,
          provider_api_readiness: false, automatic_execution_authorized: false, operator_reported_state: true,
          cost_profile_id: "mff_micro_provisional_stress_v1", exact_costs_verified: false, production_readiness: false,
          execution_authorized: false, order_paths_reachable: false, provider_connection_opened: false, armed: false,
          arm_expires_at: null, blockers: ["LOCAL_SIMULATOR_NOT_PROVIDER_EXECUTION", "SYNTHETIC_MANUAL_WORKFLOW_DEMO", "NO_PROVIDER_ORDER_TRANSMISSION"],
          verified_micro_mappings: ["MES", "MCL", "M6E"], disabled_signal_roots: ["ZN"],
        },
      },
    });
    receive({
      v: PROTOCOL_VERSION,
      type: "history_cache_status",
      payload: {
        state: "COMPLETE",
        ready_markets: 33,
        total_markets: 33,
        queued_markets: 0,
        active_market: null,
        estimated_cost_usd: null,
        estimate_expires_at: null,
        plan_id: null,
        paused: false,
        message: "Demo cache ready",
      },
    });
    setOverviewStatus("LIVE", "Deterministic 33-market demo");
    browserDemoSnapshot("ES", "1m");
  }

  window.addEventListener("pywebviewready", async () => {
    state.bridgeReady = true;
    state.browserDemo = false;
    try {
      const bootstrap = await window.pywebview.api.bootstrap();
      receive(bootstrap);
      window.setTimeout(pollDesktopEvents, 0);
    } catch (error) {
      setFocusStatus("ERROR", `Desktop bridge unavailable: ${error}`);
    }
  });

  elements.marketSearch.addEventListener("input", renderMarkets);
  [
    [elements.groupBySector, "sector"],
    [elements.groupByAlpha, "alpha_tier"],
  ].forEach(([element, mode]) => {
    element.addEventListener("click", () => {
      if (mode === "alpha_tier" && !state.alphaTierGroupingAvailable) return;
      state.uiPreferences.market_grouping_mode = mode;
      renderMarkets();
      persistUiPreferences();
    });
  });
  elements.retryFocus.addEventListener("click", retrySelectedMarket);
  elements.retryHistory.addEventListener("click", retryHistory);
  elements.historyCacheToggle.addEventListener("click", () => {
    setHistoryCachePopover(elements.historyCachePopover.hidden);
  });
  elements.historyCacheClose.addEventListener("click", () => {
    state.historyPopoverDismissedPlanId = state.historyCache?.plan_id || null;
    setHistoryCachePopover(false);
    elements.historyCacheToggle.focus();
  });
  elements.historyCacheConfirm.addEventListener("click", confirmHistoryCache);
  elements.historyCachePause.addEventListener("click", toggleHistoryCachePause);
  elements.historyCacheRetry.addEventListener("click", refreshHistoryCacheEstimate);
  elements.manualReconcile.addEventListener("click", reconcileManualState);
  elements.manualDemoStage.addEventListener("change", () => {
    if (state.mode !== "demo" || !state.executionCapability) return;
    state.executionCapability = {
      ...state.executionCapability,
      account_stage: elements.manualDemoStage.value,
      entitlement_status: "UNAVAILABLE_FOR_SIMULATED_ACCOUNT",
      provider_api_readiness: false,
      automatic_execution_authorized: false,
      blockers: [
        "MANUAL_ENTRY_REQUIRED",
        "OPERATOR_ACCOUNT_SNAPSHOT_REQUIRED",
        "SESSION_RECORD_NOT_BOUND",
        "NEWS_RECORD_NOT_BOUND",
        "PRICE_LIMIT_RECORD_NOT_BOUND",
        "OFFICIAL_EXECUTION_COSTS_UNVERIFIED",
      ],
    };
    state.manualTicketId = null;
    state.manualTicketState = "BLOCKED";
    elements.manualTicketState.textContent = "BLOCKED • MODEL CALCULATED • PREPARE A STAGE-BOUND TICKET";
    elements.manualReconcileStatus.textContent = "STALE — RECONCILIATION REQUIRED • OPERATOR REPORTED";
    renderManualControls();
    renderExecution();
  });
  elements.manualPrepareTicket.addEventListener("click", prepareManualTicket);
  elements.manualCopyOrder.addEventListener("click", async () => {
    const text = elements.manualCopyText.textContent || "";
    try {
      await navigator.clipboard.writeText(text);
      elements.manualTicketState.textContent = `${executionLabel(state.manualTicketState)} • COPIED LOCALLY • NO TRANSMISSION`;
    } catch (_error) {
      elements.manualTicketState.textContent = `${executionLabel(state.manualTicketState)} • COPY FAILED — SELECT TEXT MANUALLY`;
    }
  });
  elements.manualMarkSubmitted.addEventListener("click", () => manualTransition("OPERATOR_REPORTED_SUBMITTED", { actual_submission_time: new Date().toISOString() }));
  elements.manualRecordPartial.addEventListener("click", () => manualTransition("OPERATOR_REPORTED_PARTIALLY_FILLED", {
    partial_fills: [{ quantity: Math.max(1, Math.trunc(manualNumber(elements.manualActualQuantity) || 1)), price: manualNumber(elements.manualActualFill), time: new Date().toISOString() }],
  }));
  elements.manualRecordFill.addEventListener("click", () => manualTransition("OPERATOR_REPORTED_FILLED", {
    actual_contract: elements.manualActualContract.value.trim().toUpperCase(),
    actual_side: elements.manualActualSide.value,
    actual_quantity: Math.trunc(manualNumber(elements.manualActualQuantity) || 0),
    actual_fill_price: manualNumber(elements.manualActualFill),
    actual_stop: manualNumber(elements.manualActualStop), actual_target: manualNumber(elements.manualActualTarget),
    actual_fill_time: new Date().toISOString(), actual_fees: manualNumber(elements.manualActualFees),
  }));
  elements.manualConfirmProtection.addEventListener("click", () => manualTransition("OPERATOR_CONFIRMED_PROTECTED", {
    actual_stop: manualNumber(elements.manualActualStop), confirmed_at: new Date().toISOString(),
  }));
  elements.manualRecordRejection.addEventListener("click", () => manualTransition("OPERATOR_REPORTED_REJECTED", { actual_rejection_reason: "Operator reported rejection" }));
  elements.manualRecordCancellation.addEventListener("click", () => manualTransition("OPERATOR_REPORTED_CANCELLED", { cancelled_at: new Date().toISOString() }));
  elements.manualRecordExit.addEventListener("click", () => manualTransition("OPERATOR_REPORTED_CLOSED", { actual_exit_price: manualNumber(elements.manualActualExit), actual_exit_time: new Date().toISOString(), actual_fees: manualNumber(elements.manualActualFees) }));
  elements.manualReconcileTicket.addEventListener("click", () => manualTransition("OPERATOR_RECONCILED", { reconciliation_notes: "Operator reconciled final manual state" }));
  elements.manualStateUncertain.addEventListener("click", () => manualTransition("STATE_UNCERTAIN", { operator_notes: "Operator cannot determine current order or position state" }));
  elements.manualAbandon.addEventListener("click", () => manualTransition("ABANDONED", { operator_notes: "Operator abandoned local ticket" }));
  elements.manualCompare.addEventListener("click", compareManualActual);
  elements.fitChart.addEventListener("click", resetChartView);
  elements.fullscreenToggle.addEventListener("click", toggleFullscreen);
  elements.predictionPanelToggle.addEventListener("click", () => {
    setPanelOpen(!state.panelOpen, { persist: true });
  });
  elements.layersToggle.addEventListener("click", () => {
    const open = elements.layersMenu.hidden;
    elements.layersMenu.hidden = !open;
    elements.layersToggle.setAttribute("aria-expanded", String(open));
    elements.layersToggle.classList.toggle("active", open);
  });
  [
    [elements.layerSessions, "show_session_boundaries"],
    [elements.layerVolume, "show_volume"],
    [elements.layerPredictions, "show_predictions"],
  ].forEach(([element, key]) => {
    element.addEventListener("change", () => {
      state.uiPreferences[key] = element.checked;
      applyLayerVisibility();
      persistUiPreferences();
    });
  });
  [elements.smoothnessEfficient, elements.smoothnessSmooth, elements.smoothnessHigh].forEach((element) => {
    element.addEventListener("change", () => {
      if (!element.checked || !Object.hasOwn(VISUAL_UPDATE_HZ, element.value)) return;
      state.uiPreferences.visual_update_mode = element.value;
      state.visualUpdateConstrained = false;
      state.unhealthyPolls = 0;
      state.healthySince = null;
      renderVisualUpdateState();
      persistUiPreferences();
      syncVisualUpdateActivity();
    });
  });
  document.addEventListener("click", (event) => {
    if (!elements.layersMenu.hidden && !elements.layersToggle.parentElement.contains(event.target)) {
      elements.layersMenu.hidden = true;
      elements.layersToggle.setAttribute("aria-expanded", "false");
      elements.layersToggle.classList.remove("active");
    }
    if (
      !elements.historyCachePopover.hidden &&
      !elements.historyCacheToggle.parentElement.contains(event.target)
    ) {
      setHistoryCachePopover(false);
    }
  });
  document.addEventListener("fullscreenchange", () => {
    if (!state.browserDemo) return;
    state.fullscreen = Boolean(document.fullscreenElement);
    renderFullscreenState();
  });
  document.addEventListener("visibilitychange", () => {
    state.unhealthyPolls = 0;
    state.healthySince = null;
    syncVisualUpdateActivity();
    if (state.pollTimer !== null) {
      window.clearTimeout(state.pollTimer);
      state.pollTimer = null;
    }
    if (state.bridgeReady && !state.browserDemo && !state.pollInFlight) {
      state.pollTimer = window.setTimeout(pollDesktopEvents, 0);
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "F11") {
      event.preventDefault();
      toggleFullscreen();
      return;
    }
    if (event.key === "Escape" && !elements.layersMenu.hidden) {
      elements.layersMenu.hidden = true;
      elements.layersToggle.setAttribute("aria-expanded", "false");
      elements.layersToggle.classList.remove("active");
      elements.layersToggle.focus();
      return;
    }
    if (event.key === "Escape" && !elements.historyCachePopover.hidden) {
      setHistoryCachePopover(false);
      elements.historyCacheToggle.focus();
      return;
    }
    if (event.key === "/" && document.activeElement !== elements.marketSearch) {
      event.preventDefault();
      elements.marketSearch.focus();
      return;
    }
    if (/^[1-7]$/.test(event.key) && document.activeElement !== elements.marketSearch) {
      const timeframe = state.timeframes[Number(event.key) - 1];
      if (timeframe) chooseTimeframe(timeframe);
      return;
    }
    if (["ArrowDown", "ArrowUp"].includes(event.key) && document.activeElement !== elements.marketSearch) {
      const symbols = Array.from(elements.marketList.querySelectorAll(".market-row"))
        .map((row) => row.dataset.market)
        .filter(Boolean);
      if (!symbols.length) return;
      const currentIndex = Math.max(0, symbols.indexOf(state.selectedMarket));
      const offset = event.key === "ArrowDown" ? 1 : -1;
      const next = symbols[(currentIndex + offset + symbols.length) % symbols.length];
      if (next) chooseMarket(next);
    }
  });

  function updateClock() {
    elements.localTime.textContent = new Intl.DateTimeFormat(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(new Date());
    renderPrediction();
    renderDataHealth();
  }
  window.addEventListener("resize", () => {
    queueChartResize();
    if (!state.panelPreferenceExplicit) setPanelOpen(window.innerWidth >= 1440);
  });
  applyUiPreferences({});
  renderPrediction();
  renderDataHealth();
  renderHistoryCache();
  updateClock();
  setInterval(updateClock, 1000);

  const mode = new URLSearchParams(window.location.search).get("mode");
  if (mode === "demo") setTimeout(startBrowserDemo, 900);
})();
