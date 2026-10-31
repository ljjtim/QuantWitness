import {readFileSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

// 图源同时生成页面和SVG，保持职责、状态及说明一致。
const root = dirname(fileURLToPath(import.meta.url));
const kinds = ['overview', 'architecture', 'dataflow', 'workflow', 'sequence', 'lifecycle'];
const selected = process.argv.slice(2);
for (const kind of selected) if (!kinds.includes(kind)) throw new Error('未知图类型：' + kind);
const escape = value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
const data = kinds.map(kind => JSON.parse(readFileSync(join(root, 'sources', kind + '.json'), 'utf8')));
function svg(graph) {
  const width = 1200, height = 140 + Math.ceil(graph.nodes.length / 3) * 160;
  const locations = new Map(graph.nodes.map((n, i) => {
    const row = Math.floor(i / 3), col = row % 2 ? 2 - i % 3 : i % 3;
    return [n.id, {x: 40 + col * 400, y: 125 + row * 160}];
  }));
  const edges = graph.edges.map(edge => {
    const a = locations.get(edge.source), b = locations.get(edge.target);
    const ax = a.x + 160, ay = a.y + 40, bx = b.x + 160, by = b.y + 40;
    const horizontal = a.y === b.y;
    const x1 = horizontal ? ax + Math.sign(bx - ax) * 160 : ax;
    const y1 = horizontal ? ay : ay + Math.sign(by - ay) * 40;
    const x2 = horizontal ? bx - Math.sign(bx - ax) * 166 : bx;
    const y2 = horizontal ? by : by - Math.sign(by - ay) * 46;
    return `<path d="M${x1},${y1} L${x2},${y2}" stroke="#64748b" stroke-width="2" fill="none" marker-end="url(#arrow)"/><text x="${(x1+x2)/2}" y="${(y1+y2)/2-8}" text-anchor="middle" font-size="12" fill="#334155">${escape(edge.label)}</text>`;
  }).join('');
  const nodes = graph.nodes.map((node, i) => {
    const p = locations.get(node.id);
    return `<g><title>${escape(node.detail)}</title><rect x="${p.x}" y="${p.y}" width="320" height="80" rx="12" fill="#ffffff" stroke="#2563eb" stroke-width="2"/><text x="${p.x+160}" y="${p.y+32}" text-anchor="middle" font-size="18" font-weight="700" fill="#0f172a">${escape(node.label)}</text><text x="${p.x+160}" y="${p.y+58}" text-anchor="middle" font-size="13" fill="#475569">${i+1} · ${escape(node.id)}</text></g>`;
  }).join('');
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escape(graph.title)}" font-family="Microsoft YaHei, 微软雅黑" font-weight="400"><title>${escape(graph.title)}</title><desc>${escape(graph.question)}</desc><defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8" fill="#64748b"/></marker></defs><rect width="100%" height="100%" fill="#f1f5f9"/><text x="40" y="45" font-size="28" font-weight="700" fill="#0f172a">${escape(graph.title)}</text><text x="40" y="80" font-size="15" fill="#475569">${escape(graph.question)}</text>${edges}${nodes}</svg>`;
}
const style = `body{margin:0;background:#f1f5f9;color:#0f172a;font:16px/1.8 "Microsoft YaHei","微软雅黑",sans-serif}main{max-width:1240px;margin:auto;padding:32px 24px}a{color:#1d4ed8}nav{display:flex;gap:16px;flex-wrap:wrap}svg,img{width:100%;height:auto}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px}.card,details{background:white;border:1px solid #cbd5e1;border-radius:12px;padding:20px;margin:14px 0}summary{cursor:pointer;font-weight:bold}code{overflow-wrap:anywhere}h1{line-height:1.4}`;
const nav = data.map(g => `<a href="${g.output}.html">${escape(g.title)}</a>`).join('');
for (const graph of data.filter(g => !selected.length || selected.includes(g.diagram_type))) {
  const picture = svg(graph);
  writeFileSync(join(root, graph.output + '.svg'), picture + '\n');
  const descriptions = graph.nodes.map(node => `<details><summary>${escape(node.label)}</summary><p>${escape(node.detail)}</p></details>`).join('');
  const relations = graph.edges.map(e => `<li>${escape(graph.nodes.find(n=>n.id===e.source).label)} → ${escape(graph.nodes.find(n=>n.id===e.target).label)}：${escape(e.label)}</li>`).join('');
  writeFileSync(join(root, graph.output + '.html'), `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(graph.title)} · QuantWitness</title><style>${style}</style></head><body><main><nav><a href="index.html">图册首页</a>${nav}</nav><h1>${escape(graph.title)}</h1><p>${escape(graph.question)}</p>${picture}<h2>节点职责</h2><p>展开节点查看当前行为与边界。</p>${descriptions}<h2>交接关系</h2><ul>${relations}</ul><p><a href="../architecture.md">模块职责</a> · <a href="../runtime.md">运行与恢复</a> · <a href="../evidence.md">Result交付</a></p></main></body></html>\n`);
}
const cards = data.map(graph => `<a class="card" href="${graph.output}.html"><img src="${graph.output}.svg" alt="${escape(graph.title)}"><h2>${escape(graph.title)}</h2><p>${escape(graph.question)}</p></a>`).join('');
writeFileSync(join(root, 'index.html'), `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>QuantWitness 图册</title><style>${style}</style></head><body><main><h1>个人AI研究：从声明到Result</h1><p>一次准备与准入，执行后直接交付Result；PIT、恢复与holdout在实际消费处处理。</p><div class="grid">${cards}</div><p><a href="README.md">图册说明</a> · <a href="../index.md">文档导航</a></p></main></body></html>\n`);
