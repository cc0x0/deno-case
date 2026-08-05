# Workspace Behavioral Rules & Code Quality Guidelines

## Inline HTML Template String Guidelines
1. **Escaping Backslashes**: When writing inline `<script>` tags inside JavaScript/TypeScript template literals (backticks `` `...` ``), any string literal or regular expression containing `\n` or `\t` MUST double-escape the backslash (`/\\n/g`, `"\\n"`) to prevent the template evaluator from outputting raw physical newlines inside JS strings/regexes.
2. **Syntax Validation**: Always verify that the generated HTML script parses cleanly without `SyntaxError: Invalid regular expression` or `SyntaxError: Invalid or unexpected token`.
