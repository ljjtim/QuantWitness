import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

// README 使用简图，交互页面使用独立维护的详细图源。
const root = dirname(fileURLToPath(import.meta.url));
const [installation, ...requested] = process.argv.slice(2);
if (!installation) throw new Error('用法：node render.mjs <Archify 3.0.1 安装目录> [图类型...]');
const archify = resolve(installation);
const version = JSON.parse(readFileSync(join(archify, 'package.json'), 'utf8')).version;
if (version !== '3.0.1') throw new Error('图册使用 Archify 3.0.1，当前版本：' + version);
const outputs = {overview:'overview', architecture:'research-pipeline-architecture', dataflow:'dataflow-research-lineage', workflow:'workflow-research-task', sequence:'sequence-research-run', lifecycle:'lifecycle-research-run'};
const titles = {overview:'总体概览', architecture:'总体架构', dataflow:'数据追溯', workflow:'研究流程', sequence:'调用时序', lifecycle:'运行状态'};
const kinds = requested.length ? requested : Object.keys(outputs);
for (const kind of kinds) if (!outputs[kind]) throw new Error('未知图类型：' + kind);
function run(args) {
  const result = spawnSync(process.execPath, args, {stdio:'inherit'});
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error('图册生成失败，退出码：' + result.status);
}
run([join(root, 'render-previews.mjs'), ...kinds]);
for (const kind of kinds) {
  const target = join(root, outputs[kind] + '.html');
  run([join(archify, 'bin', 'archify.mjs'), 'render', kind === 'overview' ? 'architecture' : kind, join(root, 'sources', 'detailed', kind + '.json'), target, '--quality', 'showcase']);
  let html = readFileSync(target, 'utf8');
  // 详情浮层避开当前节点，保留再次点击取消选择的入口。
  const protectedElements = "? [svg.querySelector('[data-legend]'), container.querySelector('.diagram-nav')]";
  if (!html.includes(protectedElements)) throw new Error('Archify 详情浮层定位接口与固定版本不符');
  html = html.replace(protectedElements, "? [node, svg.querySelector('[data-legend]'), container.querySelector('.diagram-nav')]");
  const verticalAnchor = 'var nodeCenter = nodeRect.top - containerRect.top + nodeRect.height / 2;';
  html = html.replace(verticalAnchor,     "if (!mobile) { var nodeMidX = nodeRect.left - containerRect.left + nodeRect.width / 2; chip.style.left = (nodeMidX < containerRect.width / 2 ? Math.max(padding, containerRect.width - chip.offsetWidth - padding) : padding) + 'px'; }\n        " + verticalAnchor);
  const style = '<style id="quantwitness-reader-style">li{overflow-wrap:anywhere}svg text,svg tspan{font-family:"Microsoft YaHei","微软雅黑",sans-serif!important;font-weight:400}svg .node-label,svg .title{font-weight:700}.qw-navigation{display:flex;gap:12px;flex-wrap:wrap;margin:24px auto;padding:16px 24px;max-width:1200px;font:14px/1.8 "Microsoft YaHei","微软雅黑",sans-serif}.qw-navigation a{color:inherit}.qw-navigation [aria-current="page"]{font-weight:700}</style>';
  const links = Object.keys(outputs).map(k => '<a href="'+outputs[k]+'.html"'+(k===kind?' aria-current="page"':'')+'>'+titles[k]+'</a>').join('');
  html = html.replace('</head>', style + '</head>').replace('</body>', '<nav class="qw-navigation" aria-label="六张详细交互图"><a href="index.html">图册首页</a>'+links+'<a href="'+outputs[kind]+'.svg">查看简图</a></nav></body>');
  writeFileSync(target, html, 'utf8');
}
