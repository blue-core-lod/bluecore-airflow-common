/*
 * Follow Airflow's light/dark mode.
 *
 * Plugin pages render in an iframe that Airflow sandboxes with
 * "allow-same-origin", and they are served by the same API server as the UI, so
 * this can read the parent document. Airflow tracks its color mode with
 * next-themes, which resolves it to a data-theme attribute on <html>. Copying
 * that onto our own <html> lets bluecore.css react to it.
 *
 * Without this the page falls back to the prefers-color-scheme media query,
 * which only knows the operating system preference and so disagrees with
 * Airflow whenever someone uses its toggle.
 */
(function () {
  "use strict";

  var parentRoot;
  try {
    parentRoot = window.parent.document.documentElement;
  } catch (error) {
    return;
  }

  if (!parentRoot || parentRoot === document.documentElement) {
    return;
  }

  function sync() {
    var theme = parentRoot.getAttribute("data-theme");
    if (theme) {
      document.documentElement.setAttribute("data-theme", theme);
    }
  }

  sync();
  new MutationObserver(sync).observe(parentRoot, {
    attributeFilter: ["data-theme"],
  });
})();

/*
 * Makes the header's links and Logout button work inside Airflow.
 *
 * Airflow shows plugin pages in a locked-down iframe that can't change the
 * whole window. So instead, this clicks a link on Airflow's own page.
 * Logout goes to Airflow's logout URL (data-logout-url in base.html).
 */
(function () {
  "use strict";

  function topDocument() {
    try {
      return window.top !== window ? window.top.document : null;
    } catch (error) {
      return null;
    }
  }

  function navigateWholeWindow(url) {
    var doc = topDocument();
    if (!doc) {
      window.location.assign(url);
      return;
    }
    var link = doc.createElement("a");
    link.href = new URL(url, window.location.href).href;
    link.hidden = true;
    doc.body.appendChild(link);
    link.click();
    link.remove();
  }

  document.addEventListener("bluecore-logout", function (event) {
    var url = event.target.getAttribute("data-logout-url");
    if (url) {
      navigateWholeWindow(url);
    }
  });

  // Header and footer links. Ctrl/Cmd/Shift clicks work as usual.
  document.addEventListener("click", function (event) {
    if (!topDocument() || event.defaultPrevented || event.button !== 0) {
      return;
    }
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }
    var link = event.target.closest && event.target.closest('a[target="_top"][href]');
    if (!link || !/^https?:$/.test(link.protocol)) {
      return;
    }
    event.preventDefault();
    navigateWholeWindow(link.href);
  });
})();
