'use strict';
/* web transport: live feed from the wsview proxy server */

function connect() {
  const ws = new WebSocket(`ws://${location.host}/feed`);
  ws.onopen = () => setStatus(true);
  ws.onclose = () => { setStatus(false); setTimeout(connect, 1500); };
  ws.onmessage = (ev) => {
    const e = JSON.parse(ev.data);
    if (e.t === 'hello') {
      document.querySelector('#target').textContent = `${e.target} · proxy :${e.port}`;
      resetState();
      S.batch = true;
      for (const x of e.events) ingest(x);
      S.batch = false;
      updateConnSelect();
      updateCounts();
      rebuild();
      return;
    }
    ingest(e);
  };
}

connect();
