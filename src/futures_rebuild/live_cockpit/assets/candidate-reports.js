(() => {
  "use strict";
  const dialog = document.getElementById("candidate-reports-dialog");
  const content = document.getElementById("candidate-reports-content");
  const number = value => value === null || value === undefined ? "Unavailable" : new Intl.NumberFormat("en-US", {maximumFractionDigits: 6}).format(value);
  function node(tag, text) {
    const result = document.createElement(tag);
    if (text !== undefined) result.textContent = String(text);
    return result;
  }
  function detail(title, value, parent = content) {
    const section = node("details");
    section.append(node("summary", title), node("pre", JSON.stringify(value, null, 2)));
    parent.append(section);
  }
  function table(headers, rows) {
    const wrap = node("div");
    wrap.className = "candidate-report-table";
    const grid = node("table");
    const head = node("tr");
    headers.forEach(title => { const th = node("th", title); th.scope = "col"; head.append(th); });
    const thead = node("thead"); thead.append(head); grid.append(thead);
    const body = node("tbody");
    rows.forEach(values => { const row = node("tr"); values.forEach(value => row.append(node("td", value))); body.append(row); });
    grid.append(body); wrap.append(grid); content.append(wrap);
  }
  function render(view) {
    content.replaceChildren();
    dialog.style.boxSizing = "border-box";
    dialog.style.maxWidth = "94vw";
    if (view.study_summaries?.length) {
      document.getElementById("candidate-reports-title").textContent = "Research results";
      dialog.querySelector(":scope > p").textContent = "Predictive and economic studies retain distinct measures. Development results grant no confirmation or trading authority.";
    }
    for (const study of view.study_summaries || []) {
      content.append(node("h3", "Study result"));
      table(["Field", "Result"], study.rows);
      const grid = content.lastElementChild.querySelector("table");
      grid.style.tableLayout = "fixed";
      grid.querySelector("th").style.width = "24%";
      for (const cell of content.lastElementChild.querySelectorAll("td")) cell.style.overflowWrap = "anywhere";
    }
    if ((!view.reports || !view.reports.length) && view.study_summaries?.length) return;
    if (!view.reports || !view.reports.length) {
      content.append(node("p", view.unavailable_reason || "No candidate reports are available."));
      return;
    }
    content.append(node("p", `Recorded attempts: ${view.history.observed_attempt_count}. Historical search and effective independent trial count remain unknown. Rows retain their evaluation stage; there is no score ranking.`));
    const names = new Map(view.reports.map(r => [r.report_id, r.identity.candidate_id || "Unknown candidate"]));
    table(["Candidate", "Evaluation stage", "Execution", "Dates available / expected", "Gross USD", "Costs USD", "Net USD", "Mean USD/date", "Drawdown USD", "Underwater dates"],
      view.reports.map(r => [names.get(r.report_id), r.scope.evaluation_stage,
        r.execution.status + (r.execution.valid_no_trade ? " · valid no trade" : "") + (r.execution.status === "COMPLETE" && !r.metric_eligibility.complete_reference_economics ? " · accounting incomplete" : ""),
        `${r.coverage.available_dates} / ${r.coverage.expected_dates}`, ...["gross_usd", "cost_usd", "net_usd", "mean_daily_net_usd"].map(k => number(r.metrics[k].value)),
        number(r.drawdown.maximum_depth_usd), number(r.drawdown.time_underwater_dates)]));
    content.append(node("h3", "Paired comparisons"));
    if (!view.comparisons.length) content.append(node("p", "No incumbent or benchmark comparison has been declared."));
    for (const c of view.comparisons) {
      if (c.status === "UNAVAILABLE") { detail("Unavailable comparison", c); continue; }
      content.append(node("p", `${names.get(c.candidate_report_id)} versus ${names.get(c.incumbent_report_id)} · ${c.role} · ${c.status} · ${c.attribution}`));
      table(["A mean USD/date", "B mean USD/date", "B − A USD/date", "Cumulative B − A USD", "Matched / expected dates"], [[
        number(c.statistics.incumbent_mean_daily_usd), number(c.statistics.candidate_mean_daily_usd), number(c.statistics.mean_daily_difference_usd),
        number(c.statistics.cumulative_difference_usd), `${c.coverage.matched_dates} / ${c.coverage.expected_dates}`]]);
      detail("Coverage, attribution, fold differences, uncertainty and claim limits", c);
    }
    for (const r of view.reports) {
      content.append(node("h3", names.get(r.report_id)));
      content.append(node("p", `${r.information_use.stage_interpretation} Independence: ${r.information_use.independence}.`));
      if (!r.comparison_eligibility.descriptive) content.append(node("p", `Comparison unavailable: ${r.comparison_eligibility.reasons.join("; ")}`));
      if (r.execution.reason) content.append(node("p", r.execution.reason));
      if (r.drawdown.current_episode) content.append(node("p", `Unrecovered drawdown since ${r.drawdown.current_episode.start_date}: ${r.drawdown.current_episode.underwater_dates} evaluation dates underwater.`));
      detail("Policy, lineage, search history, dates and provenance", {identity: r.identity, policy: r.policy, scope: r.scope, lineage: r.lineage, provenance: r.provenance});
      detail("Gross/net accounting, metric availability and coverage", {metrics: r.metrics, coverage: r.coverage, eligibility: r.comparison_eligibility, partial_diagnostic: r.partial_diagnostic});
      detail("Folds, drawdowns, concentration, costs and sensitivity", {folds: r.folds, drawdown: r.drawdown, concentration: r.profit_concentration, trades: r.trade_diagnostics, cost_drift: r.execution_cost_drift});
    }
    detail("Evaluation-stage definitions", view.stage_definitions);
  }
  document.getElementById("candidate-reports-open").addEventListener("click", async () => {
    dialog.showModal();
    try {
      const view = window.l0CandidateReportDemo || (window.pywebview?.api?.candidate_reports ? await window.pywebview.api.candidate_reports() : {reports: []});
      render(view);
    } catch (_) {
      content.replaceChildren(node("p", "Candidate reports unavailable. No research action was attempted."));
    }
  });
  document.getElementById("candidate-reports-close").addEventListener("click", () => dialog.close());
  if (window.l0CandidateReportDemo) { render(window.l0CandidateReportDemo); dialog.showModal(); }
})();
