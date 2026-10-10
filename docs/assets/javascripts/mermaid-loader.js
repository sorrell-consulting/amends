/* Material 9.7.7 calls initialize and render. Delay the pinned local bundle
   until its first diagram while retaining Material's renderer and theme CSS. */
(() => {
  const url = new URL("../vendor/mermaid-11.12.1.min.js", document.currentScript.src);
  let options;
  let library;
  const deferred = {
    initialize(config) { options = config; },
    async render(id, source) {
      library ??= new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = url;
        script.onload = () => {
          // The UMD bundle replaces this adapter with the real Mermaid object.
          window.mermaid.initialize(options);
          resolve(window.mermaid);
        };
        script.onerror = () => {
          library = undefined;
          window.mermaid = deferred;
          script.remove();
          reject(new Error("Unable to load the local Mermaid bundle"));
        };
        document.head.append(script);
      });
      try { return await (await library).render(id, source); }
      catch (error) {
        console.warn("Diagram unavailable; showing its source.", error);
        const escaped = source.replace(/[&<>]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;"}[c]));
        return {svg: `<p>Diagram unavailable. Its source follows:</p><pre>${escaped}</pre>`};
      }
    }
  };
  window.mermaid = deferred;
})();
