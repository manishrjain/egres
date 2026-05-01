# Local Agent Notes

- Keep every line you modify or generate at 80 characters or fewer.
- When adding or changing non-trivial logic, add comments that explain
  intent and invariants, not just mechanics.
- Commenting practice:
  - Keep comments concise and close to the relevant block.
  - Explain why the code exists and what assumptions matter.
  - Mention edge cases when they are not obvious.
  - Avoid redundant comments that restate the code line by line.
- Generated code:
  - Change generators first, then regenerate checked-in outputs.
  - Keep generated output formatted and consistent with the generator.
  - Run focused tests for every generated module touched.
