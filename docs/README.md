# Egres documentation

The pages in this directory are the documentation site at egresql.com. The site
itself (theme, landing page, build) lives in manishrjain/docs-egres , which
pulls this directory in at build time; nothing here needs to know about it.

Layout:

- `index.md`, `quickstart.md`, `postgres-compatibility.md`, `benchmarks.md`:
  the top-level pages.
- `design/`: one page per section of the launch post, in the post's order.
- `figures/`: the SVG figures the pages embed.

Writing a page:

- Start with YAML frontmatter carrying at least `title:`; `description:` and
  `sidebar: { label, order }` are optional. A `.md` file without a `title`
  is left out of the site, so design notes such as `transactions.md` can
  live here untouched.
- Link to other pages by relative path to the `.md` file, for example
  `[branching](design/branching.md)` or `[benchmarks](../benchmarks.md)`.
  Embed figures by relative path too: `<img src="../figures/x.svg">`. Both
  render on GitHub, and the site build rewrites them to its own URLs.
- Pages must also parse as MDX, so wrap a bare `<` or `{` in backticks and
  use fenced code blocks, not indented ones.
- A page's "last updated" date on the site is its last commit here.

A push to `main` that touches this directory triggers a rebuild of the site
through `.github/workflows/docs.yml`.
