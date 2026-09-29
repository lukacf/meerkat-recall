// Validate the published documentation without starting a server or model.
import {compile, createProcessor} from '@mdx-js/mdx';
import GithubSlugger from 'github-slugger';
import remarkFrontmatter from 'remark-frontmatter';
import remarkGfm from 'remark-gfm';
import {visit} from 'unist-util-visit';
import {parse as parseYaml} from 'yaml';
import {readFile, readdir, stat} from 'node:fs/promises';
import {dirname, extname, join, relative, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const docs = join(root, 'docs');
const options = {remarkPlugins: [remarkFrontmatter, remarkGfm]};
const processor = createProcessor(options);
const errors = [];
const pages = new Map();
let jsonExamples = 0;
let localLinks = 0;
const fail = (file, message) => errors.push(`${relative(root, file)}: ${message}`);

async function filesIn(dir) {
  const files = [];
  for (const entry of await readdir(dir, {withFileTypes: true})) {
    if (entry.name.startsWith('.') || entry.name === 'node_modules') continue;
    const path = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...await filesIn(path));
    else if (/\.mdx?$/.test(entry.name)) files.push(path);
  }
  return files.sort();
}
async function exists(path) {
  try { return (await stat(path)).isFile(); } catch { return false; }
}
function nodeText(node) {
  return node.value ?? node.children?.map(nodeText).join('') ?? '';
}

const files = [
  ...await filesIn(docs),
  ...['README.md', 'CONTRIBUTING.md', 'DEV.md', 'QUICK-DEV.md', 'RELEASE.md',
    'CHANGELOG.md', 'elephant-local-helper/README.md'].map(path => join(root, path)),
];
for (const file of files) {
  const source = await readFile(file, 'utf8');
  if (/[\u2013\u2014]/u.test(source)) fail(file, 'use ASCII hyphens');
  let tree;
  try {
    if (extname(file) === '.mdx') {
      await compile(source, options);
      tree = processor.parse(source);
      const frontmatter = tree.children.find(node => node.type === 'yaml');
      const metadata = frontmatter && parseYaml(frontmatter.value);
      if (!metadata || !['title', 'description'].every(key =>
        typeof metadata[key] === 'string' && metadata[key].trim())) {
        fail(file, 'frontmatter requires a title and description');
      }
      if (tree.children.some(node => node.type === 'heading' && node.depth === 1)) {
        fail(file, 'use frontmatter for the page title, then level-2 headings');
      }
    } else {
      // Markdown can contain angle-bracket placeholders that are not MDX.
      const {unified} = await import('unified');
      const {default: remarkParse} = await import('remark-parse');
      tree = unified().use(remarkParse).use(remarkGfm).parse(source);
    }
  } catch (error) {
    fail(file, `parse: ${error.message}`);
    continue;
  }
  const slugger = new GithubSlugger();
  const anchors = new Set();
  const links = [];
  visit(tree, node => {
    if (node.type === 'heading') anchors.add(slugger.slug(nodeText(node)));
    if (node.type === 'link' || node.type === 'image' || node.type === 'definition') {
      links.push(node.url);
    }
    if (node.type === 'html' && /\b(?:href|src)\s*=/i.test(node.value)) {
      fail(file, 'use Markdown links/images instead of raw HTML href/src attributes');
    }
    if (node.type === 'mdxJsxFlowElement' || node.type === 'mdxJsxTextElement') {
      for (const attr of node.attributes) {
        if (attr.type !== 'mdxJsxAttribute') continue;
        if (attr.name === 'href' || attr.name === 'src') {
          if (typeof attr.value === 'string') links.push(attr.value);
          else fail(file, `use a quoted string for ${attr.name} so its target can be checked`);
        }
        if (attr.name === 'id' && typeof attr.value === 'string') anchors.add(attr.value);
      }
      if (node.name === 'Steps') {
        const children = node.children.filter(child =>
          child.type !== 'mdxFlowExpression' && !(child.type === 'text' && !child.value.trim()));
        if (children.some(child => child.name !== 'Step')) fail(file, 'Steps must contain Step components');
      }
    }
    if (node.type === 'code' && node.lang === 'json') {
      jsonExamples++;
      try { JSON.parse(node.value); }
      catch (error) { fail(file, `JSON example at line ${node.position.start.line}: ${error.message}`); }
    }
  });
  pages.set(file, {source, anchors, links});
}

const configPath = join(docs, 'docs.json');
const config = JSON.parse(await readFile(configPath, 'utf8'));
const navigation = [];
function collectPages(value) {
  if (Array.isArray(value)) value.forEach(collectPages);
  else if (value && typeof value === 'object') {
    if (Array.isArray(value.pages)) navigation.push(...value.pages.filter(x => typeof x === 'string'));
    Object.values(value).forEach(collectPages);
  }
}
collectPages(config.navigation);
const navSet = new Set(navigation);
if (navSet.size !== navigation.length) fail(configPath, 'duplicate navigation pages');
for (const page of navigation) {
  if (!await exists(join(docs, `${page}.mdx`))) fail(configPath, `missing page ${page}`);
}
for (const file of files.filter(path => path.endsWith('.mdx'))) {
  const page = relative(docs, file).replace(/\.mdx$/, '');
  if (!navSet.has(page)) fail(configPath, `page absent from navigation: ${page}`);
}
for (const [file, page] of pages) {
  for (const link of page.links) {
    if (/^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(link)) continue;
    localLinks++;
    const [destination, encodedAnchor] = link.split('#', 2);
    const path = decodeURIComponent(destination.split('?')[0]);
    const target = path ? (path.startsWith('/') ? join(docs, path) : resolve(dirname(file), path)) : file;
    const candidates = [target, `${target}.mdx`, `${target}.md`];
    let found;
    for (const candidate of candidates) if (await exists(candidate)) { found = candidate; break; }
    if (!found) { fail(file, `broken local link ${link}`); continue; }
    if (encodedAnchor && pages.has(found) && !pages.get(found).anchors.has(decodeURIComponent(encodedAnchor))) {
      fail(file, `unknown heading in ${link}`);
    }
  }
}

// Keep the reference's core tool coverage and profile labels tied to Rust owners.
const handlerDir = join(root, 'crates/mcp/src/handlers');
const handlerFiles = (await readdir(handlerDir)).filter(path => path.endsWith('.rs'));
const handlers = (await Promise.all(handlerFiles.map(path => readFile(join(handlerDir, path), 'utf8')))).join('\n');
const core = new Set([...handlers.matchAll(/fn name\(&self\) -> &'static str \{\s*"([^"]+)"/g)].map(m => m[1]));
const toolPath = join(docs, 'mcp/tools.mdx');
const toolDoc = await readFile(toolPath, 'utf8');
const entries = [...toolDoc.matchAll(/^### (\w+)\n\*\*Profiles:\*\* ([^\n]+)/gm)];
const documented = new Set(entries.map(m => m[1]));
for (const name of core) if (!documented.has(name)) fail(toolPath, `missing tool ${name}`);
const profileSource = await readFile(join(root, 'crates/mcp/src/profile.rs'), 'utf8');
const profiles = Object.fromEntries(Object.entries({D: 'DEFAULT', A: 'AGENT', M: 'MAINTENANCE', P: 'PIPELINE'}).map(([key, prefix]) => {
  const match = profileSource.match(new RegExp(`pub const ${prefix}_TOOLS:.*?= &\\[([\\s\\S]*?)\\];`));
  if (!match) throw new Error(`Missing ${prefix}_TOOLS constant`);
  return [key, new Set([...match[1].matchAll(/"([a-z_]+)"/g)].map(m => m[1]))];
}));
for (const [, name, labels] of entries) {
  if (!core.has(name)) continue;
  // The Default dispatcher substitutes this convenience handler for deeper reads.
  const expected = name === 'get_provenance' ? ['D'] :
    [...Object.keys(profiles).filter(key => profiles[key].has(name)), 'F'];
  const actual = labels.match(/\b[DAMPF]\b/g) ?? [];
  if ([...new Set(actual)].sort().join() !== expected.sort().join()) {
    fail(toolPath, `${name}: expected profiles ${expected.join(', ')}, found ${actual.join(', ')}`);
  }
}

console.log(JSON.stringify({files: files.length, pages: navSet.size, jsonExamples, localLinks,
  coreTools: core.size, errors}, null, 2));
process.exitCode = errors.length ? 1 : 0;
