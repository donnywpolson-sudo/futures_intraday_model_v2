(() => {
  "use strict";

  const formatters = new Map();

  function formatter(locale, key, options) {
    const cacheKey = `${locale || "default"}:${key}`;
    if (!formatters.has(cacheKey)) {
      formatters.set(cacheKey, new Intl.DateTimeFormat(locale || undefined, options));
    }
    return formatters.get(cacheKey);
  }

  function timestampDate(time) {
    if (typeof time !== "number" || !Number.isFinite(time)) return null;
    return new Date(time * 1000);
  }

  function formatLocalCrosshairTime(time, locale) {
    const date = timestampDate(time);
    if (!date) return "";
    return formatter(locale, "crosshair", {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
      timeZoneName: "short",
    }).format(date);
  }

  function formatLocalTickMark(time, tickMarkType, locale) {
    const date = timestampDate(time);
    if (!date) return null;
    const definitions = {
      0: ["year", { year: "numeric" }],
      1: ["month", { month: "short" }],
      2: ["day", { month: "short", day: "numeric" }],
      3: ["minute", { hour: "2-digit", minute: "2-digit", hourCycle: "h23" }],
      4: ["second", { hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" }],
    };
    const definition = definitions[Number(tickMarkType)];
    if (!definition) return null;
    return formatter(locale, definition[0], definition[1]).format(date);
  }

  window.CockpitTime = Object.freeze({
    formatLocalCrosshairTime,
    formatLocalTickMark,
  });
})();
