/* Chart.js data mirrors the hand-derived fixtures; scripts/check_docs.py checks it. */
(() => {
  const inventory = {
    "labels": ["Opening", "Day 1", "Day 2", "Day 3"],
    "threshold": 60,
    "original": [100, 50, 30, 80],
    "corrected": [100, 70, 50, 100],
    "crossings": {"original": 1, "corrected": 2}
  };
  let charts = [];
  let generation = 0;
  let chartLibrary;
  const chartURL = new URL("../vendor/chart-4.5.1.umd.min.js", document.currentScript.src);

  async function renderInventory() {
    const current = ++generation;
    charts.forEach(chart => chart.destroy());
    charts = [];
    if (!document.getElementById("inventory-original")) return;
    if (typeof Chart === "undefined") {
      chartLibrary ??= new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = chartURL;
        script.onload = resolve;
        script.onerror = () => {
          chartLibrary = undefined;
          script.remove();
          reject(new Error("Unable to load the local Chart.js bundle"));
        };
        document.head.append(script);
      });
      try { await chartLibrary; }
      catch (error) { console.warn("Charts unavailable; the table contains all values.", error); return; }
    }
    // A navigation or palette change may have superseded this pending load.
    if (current !== generation) return;
    const style = getComputedStyle(document.body);
    const text = style.getPropertyValue("--md-default-fg-color").trim();
    const grid = style.getPropertyValue("--sc-chart-grid").trim();
    const balance = style.getPropertyValue("--sc-chart-balance").trim();
    const threshold = style.getPropertyValue("--sc-chart-threshold").trim();
    Chart.defaults.font.family = style.fontFamily;
    for (const name of ["original", "corrected"]) {
      const canvas = document.getElementById(`inventory-${name}`);
      if (!canvas) continue;
      // Also safe if another lifecycle invocation already used this canvas.
      Chart.getChart(canvas)?.destroy();
      const crossing = inventory.crossings[name];
      charts.push(new Chart(canvas, {
        type: "line",
        data: {
          labels: inventory.labels,
          datasets: [{
            label: "Inventory (units)",
            data: inventory[name],
            borderColor: balance,
            backgroundColor: balance,
            borderWidth: 2,
            pointRadius: inventory.labels.map((_, i) => i === crossing ? 8 : 4),
            pointStyle: inventory.labels.map((_, i) => i === crossing ? "triangle" : "circle"),
            pointHoverRadius: 9,
            tension: 0
          }, {
            label: "Threshold: 60 units",
            data: inventory.labels.map(() => inventory.threshold),
            borderColor: threshold,
            borderWidth: 2,
            borderDash: [6, 5],
            pointRadius: 0,
            pointHoverRadius: 0
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          interaction: {mode: "index", intersect: false},
          plugins: {
            legend: {labels: {color: text, boxWidth: 20, font: {size: 12}}},
            tooltip: {callbacks: {
              afterBody: items => {
                const i = items[0]?.dataIndex;
                if (i === crossing) return "Downward crossing: alert";
                if (name === "corrected" && i === 1) return "Original Day 1 alert withdrawn";
                return "";
              },
              label: item => `${item.datasetIndex === 0 ? "Inventory" : "Threshold"}: ${item.parsed.y} units`
            }}
          },
          scales: {
            y: {min: 0, max: 120, title: {display: true, text: "Inventory (units)", color: text},
              ticks: {stepSize: 20, color: text}, grid: {color: grid}},
            x: {title: {display: true, text: "Opening / daily close", color: text},
              ticks: {color: text, maxRotation: 0, autoSkip: false}, grid: {display: false}}
          }
        }
      }));
    }
  }
  // Material emits on initial load and each instant navigation, including back/forward.
  if (typeof document$ !== "undefined") document$.subscribe(renderInventory);
  else if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", renderInventory);
  else renderInventory();
  new MutationObserver(renderInventory).observe(document.body, {
    attributes: true, attributeFilter: ["data-md-color-scheme"]
  });
})();
