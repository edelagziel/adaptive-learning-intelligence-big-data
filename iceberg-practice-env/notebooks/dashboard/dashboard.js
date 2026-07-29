(function () {
  "use strict";

  const DATA_PATH = "data/dashboard_data.json";

  const elements = {
    generatedAt: document.getElementById("generatedAt"),
    refreshButton: document.getElementById("refreshButton"),
    kpiGrid: document.getElementById("kpiGrid"),
    weakestConcepts: document.getElementById("weakestConcepts"),
    progressChart: document.getElementById("progressChart"),
    learnerRisk: document.getElementById("learnerRisk"),
    learningGap: document.getElementById("learningGap"),
    qualitySummary: document.getElementById("qualitySummary"),
    pipelineStatus: document.getElementById("pipelineStatus"),
    dataStatus: document.getElementById("dataStatus"),
    errorPanel: document.getElementById("errorPanel"),
    errorMessage: document.getElementById("errorMessage")
  };

  function clear(node) {
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function text(value) {
    return value === null || value === undefined || value === "" ? "Not available" : String(value);
  }

  function toFiniteNumber(value) {
    if (value === null || value === undefined || value === "") {
      return null;
    }

    const numericValue = Number(value);
    return Number.isFinite(numericValue) ? numericValue : null;
  }

  function safeFractionDigits(value, fallback) {
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) {
      return fallback;
    }
    return Math.max(0, Math.min(20, Math.trunc(numericValue)));
  }

  function safeFractionOptions(minimumDigits, maximumDigits) {
    const maximumFractionDigits = safeFractionDigits(maximumDigits, 0);
    const minimumFractionDigits = Math.min(
      safeFractionDigits(minimumDigits, 0),
      maximumFractionDigits
    );

    return {
      minimumFractionDigits,
      maximumFractionDigits
    };
  }

  function formatNumber(value, maximumDigits, minimumDigits) {
    const numericValue = toFiniteNumber(value);
    if (numericValue === null) {
      return "Not available";
    }

    try {
      return numericValue.toLocaleString(
        undefined,
        safeFractionOptions(minimumDigits, maximumDigits)
      );
    } catch (error) {
      return String(numericValue);
    }
  }

  function formatPercent(value, maximumDigits, minimumDigits) {
    const numericValue = toFiniteNumber(value);
    if (numericValue === null) {
      return "Not available";
    }

    return `${formatNumber(numericValue * 100, maximumDigits, minimumDigits)}%`;
  }

  function emptyState(message) {
    const div = document.createElement("div");
    div.className = "empty-state";
    div.textContent = message;
    return div;
  }

  function badgeClass(status) {
    const normalized = String(status || "").toLowerCase();
    if (normalized === "available" || normalized === "success" || normalized === "pass") {
      return "badge good";
    }
    if (normalized === "partial" || normalized === "warning") {
      return "badge warning";
    }
    if (normalized === "fail" || normalized === "failed") {
      return "badge fail";
    }
    return "badge";
  }

  function renderKpis(kpis) {
    const items = [
      ["Total learners", formatNumber(kpis.total_learners, 0), "Current learner dimension"],
      ["Learning events", formatNumber(kpis.total_learning_events, 0), "Gold fact events"],
      ["Practice attempts", formatNumber(kpis.total_practice_attempts, 0), "Gold fact attempts"],
      ["Weak concepts", formatNumber(kpis.weak_concepts, 0), "Distinct topics below mastery 0.6"],
      ["Average mastery", formatPercent(kpis.average_mastery, 1), "Current concept state"],
      ["At-risk learners", formatNumber(kpis.at_risk_learners, 0), "Latest ML predictions"]
    ];

    clear(elements.kpiGrid);
    items.forEach(([label, value, note]) => {
      const card = document.createElement("article");
      card.className = "kpi-card";

      const labelNode = document.createElement("div");
      labelNode.className = "kpi-label";
      labelNode.textContent = label;

      const valueNode = document.createElement("div");
      valueNode.className = "kpi-value";
      valueNode.textContent = value;

      const noteNode = document.createElement("div");
      noteNode.className = "kpi-note";
      noteNode.textContent = note;

      card.append(labelNode, valueNode, noteNode);
      elements.kpiGrid.appendChild(card);
    });
  }

  function renderWeakestConcepts(rows) {
    clear(elements.weakestConcepts);
    if (!rows || rows.length === 0) {
      elements.weakestConcepts.appendChild(emptyState("No weakest-concept rows are available yet."));
      return;
    }

    rows.forEach((row) => {
      const mastery = toFiniteNumber(row.mastery_score) || 0;
      const item = document.createElement("div");
      item.className = "bar-row";

      const label = document.createElement("div");
      label.className = "bar-label";
      label.title = text(row.concept);
      label.textContent = text(row.concept);

      const track = document.createElement("div");
      track.className = "bar-track";
      const fill = document.createElement("div");
      fill.className = "bar-fill";
      fill.style.width = `${Math.max(0, Math.min(100, mastery * 100))}%`;
      track.appendChild(fill);

      const value = document.createElement("div");
      value.className = "bar-value";
      value.textContent = formatPercent(row.mastery_score, 1);

      item.append(label, track, value);
      elements.weakestConcepts.appendChild(item);
    });
  }

  function renderProgress(rows) {
    clear(elements.progressChart);
    if (!rows || rows.length === 0) {
      elements.progressChart.appendChild(emptyState("No learning-progress periods are available yet."));
      return;
    }

    const width = 680;
    const height = 240;
    const padding = 34;
    const values = rows.map((row) => toFiniteNumber(row.average_score)).filter((value) => value !== null);
    if (values.length === 0) {
      elements.progressChart.appendChild(emptyState("Progress rows do not yet contain average scores."));
      return;
    }

    const points = rows.map((row, index) => {
      const x = rows.length === 1 ? width / 2 : padding + index * ((width - padding * 2) / (rows.length - 1));
      const score = toFiniteNumber(row.average_score);
      const y = score === null ? height - padding : height - padding - Math.max(0, Math.min(1, score)) * (height - padding * 2);
      return { x, y, row };
    });

    const polyline = points.map((point) => `${point.x},${point.y}`).join(" ");
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", "Learning progress line chart");
    svg.innerHTML = `
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" stroke="#dbe3ee" />
      <line x1="${padding}" y1="${padding}" x2="${padding}" y2="${height - padding}" stroke="#dbe3ee" />
      <polyline fill="none" stroke="#0f766e" stroke-width="4" points="${polyline}" />
    `;

    points.forEach((point, index) => {
      const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      circle.setAttribute("cx", point.x);
      circle.setAttribute("cy", point.y);
      circle.setAttribute("r", "5");
      circle.setAttribute("fill", "#115e59");
      svg.appendChild(circle);

      if (index === 0 || index === points.length - 1) {
        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", point.x);
        label.setAttribute("y", height - 8);
        label.setAttribute("text-anchor", index === 0 ? "start" : "end");
        label.setAttribute("class", "axis-label");
        label.textContent = text(point.row.period);
        svg.appendChild(label);
      }
    });

    elements.progressChart.appendChild(svg);
  }

  function renderTable(container, columns, rows, emptyMessage) {
    clear(container);
    if (!rows || rows.length === 0) {
      container.appendChild(emptyState(emptyMessage));
      return;
    }

    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    columns.forEach((column) => {
      const th = document.createElement("th");
      th.textContent = column.label;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((column) => {
        const td = document.createElement("td");
        try {
          td.textContent = column.format ? column.format(row[column.key], row) : text(row[column.key]);
        } catch (error) {
          td.textContent = "Not available";
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    table.append(thead, tbody);
    container.appendChild(table);
  }

  function renderRisk(rows) {
    renderTable(
      elements.learnerRisk,
      [
        { key: "learner_id", label: "Learner" },
        { key: "risk_score", label: "Risk", format: (value) => formatPercent(value, 1) },
        { key: "risk_level", label: "Level" },
        { key: "main_signal", label: "Signal" }
      ],
      rows,
      "No ML prediction rows are available yet."
    );
  }

  function renderGap(rows) {
    renderTable(
      elements.learningGap,
      [
        { key: "concept", label: "Concept" },
        { key: "confidence_before", label: "Before", format: (value) => formatNumber(value, 0) },
        { key: "practice_score", label: "Practice", format: (value) => formatPercent(value, 1) },
        { key: "confidence_after", label: "After", format: (value) => formatNumber(value, 0) },
        { key: "gap_level", label: "Gap" }
      ],
      rows,
      "No illusion-of-learning gap rows are available yet."
    );
  }

  function renderQuality(summary) {
    clear(elements.qualitySummary);
    const items = [
      ["PASS", summary.pass_count, "good"],
      ["WARNING", summary.warning_count, "warning"],
      ["FAIL", summary.fail_count, "fail"],
      [summary.quarantine_count_label === "unresolved_quarantined_rows" ? "Unresolved quarantine" : "Quarantine rows", summary.quarantined_rows, "warning"]
    ];

    items.forEach(([label, value, tone]) => {
      const tile = document.createElement("div");
      tile.className = "quality-tile";
      const badge = document.createElement("span");
      badge.className = `badge ${tone}`;
      badge.textContent = label;
      const strong = document.createElement("strong");
      strong.textContent = formatNumber(value, 0);
      tile.append(badge, strong);
      elements.qualitySummary.appendChild(tile);
    });
  }

  function renderPipeline(rows) {
    clear(elements.pipelineStatus);
    if (!rows || rows.length === 0) {
      elements.pipelineStatus.appendChild(emptyState("No pipeline status rows are available."));
      return;
    }

    rows.forEach((row) => {
      const item = document.createElement("div");
      item.className = "status-item";
      const title = document.createElement("div");
      title.className = "status-title";
      const name = document.createElement("span");
      name.textContent = text(row.component);
      const badge = document.createElement("span");
      badge.className = badgeClass(row.status);
      badge.textContent = text(row.status);
      title.append(name, badge);
      const details = document.createElement("p");
      details.className = "kpi-note";
      details.textContent = text(row.details);
      item.append(title, details);
      elements.pipelineStatus.appendChild(item);
    });
  }

  function renderSection(sectionName, container, renderCallback) {
    try {
      renderCallback();
    } catch (error) {
      clear(container);
      container.appendChild(emptyState(`${sectionName} could not be rendered.`));
      console.error(`${sectionName} render failed`, error);
    }
  }

  async function loadData() {
    elements.errorPanel.hidden = true;
    elements.dataStatus.textContent = "Loading";
    elements.dataStatus.className = "badge";

    try {
      const response = await fetch(`${DATA_PATH}?t=${Date.now()}`, { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`Could not load ${DATA_PATH}: HTTP ${response.status}`);
      }
      const data = await response.json();
      renderSection("KPI cards", elements.kpiGrid, () => renderKpis(data.kpis || {}));
      renderSection("Weakest concepts", elements.weakestConcepts, () => renderWeakestConcepts(data.weakest_concepts || []));
      renderSection("Learning progress", elements.progressChart, () => renderProgress(data.learning_progress || []));
      renderSection("ML risk", elements.learnerRisk, () => renderRisk(data.learner_risk || []));
      renderSection("Learning gap", elements.learningGap, () => renderGap(data.learning_gap || []));
      renderSection("Data quality", elements.qualitySummary, () => renderQuality(data.quality_summary || {}));
      renderSection("Pipeline status", elements.pipelineStatus, () => renderPipeline(data.pipeline_status || []));

      elements.generatedAt.textContent = `Generated: ${text(data.generated_at)}`;
      elements.dataStatus.textContent = text(data.status);
      elements.dataStatus.className = badgeClass(data.status);
    } catch (error) {
      elements.errorPanel.hidden = false;
      elements.errorMessage.textContent = `${error.message}. Run the dashboard export job, then refresh this page.`;
      elements.generatedAt.textContent = "Generated: not loaded";
      elements.dataStatus.textContent = "Unavailable";
      elements.dataStatus.className = "badge fail";

      renderKpis({});
      [elements.weakestConcepts, elements.progressChart, elements.learnerRisk, elements.learningGap, elements.qualitySummary, elements.pipelineStatus].forEach((node) => {
        clear(node);
        node.appendChild(emptyState("Dashboard data has not been loaded."));
      });
    }
  }

  elements.refreshButton.addEventListener("click", loadData);
  loadData();
}());
