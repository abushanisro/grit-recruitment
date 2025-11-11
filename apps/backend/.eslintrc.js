module.exports = {
  parser: '@typescript-eslint/parser',
  parserOptions: {
    project: 'tsconfig.json',
    tsconfigRootDir: __dirname,
    sourceType: 'module',
  },
  plugins: ['@typescript-eslint/eslint-plugin', 'function-name'],
  extends: [
    'plugin:@typescript-eslint/strict-type-checked',
    'plugin:prettier/recommended',
  ],
  root: true,
  env: {
    node: true,
    jest: true,
  },
  ignorePatterns: ['.eslintrc.js'],
  rules: {
    'prettier/prettier': [
      'error',
      {
        endOfLine: 'auto',
      },
    ],
    '@typescript-eslint/no-shadow': 'error',
    'max-params': 'error',
    'no-nested-ternary': 'error',
    yoda: 'error',
    'no-await-in-loop': 'error',
    '@typescript-eslint/naming-convention': [
      'error',
      {
        selector: 'variable',
        format: ['camelCase', 'UPPER_CASE'],
      },
      {
        selector: 'typeLike',
        format: ['PascalCase'],
      },
      {
        selector: 'variable',
        types: ['number', 'string', 'array'],
        modifiers: ['global'],
        format: ['UPPER_CASE'],
        leadingUnderscore: 'allow',
        trailingUnderscore: 'forbid',
      },
      {
        selector: 'function',
        format: ['camelCase'],
      },
      {
        selector: 'variable',
        modifiers: ['const'],
        types: ['function'],
        format: ['camelCase'],
      },
    ],
    complexity: ['error', 15],
    'max-depth': ['error', 5],
    'max-lines': ['error', { max: 2000 }],
    'no-multi-spaces': 'error',
    'no-useless-return': 'error',
    'no-else-return': 'error',
    'no-return-assign': 'error',
    'no-multi-str': 'error',
  },
  overrides: [
    {
      files: ['main.ts'],
      rules: {
        'function-name/starts-with-verb': 'off',
      },
    },
    {
      files: ['**/*.module.ts'],
      rules: {
        '@typescript-eslint/no-extraneous-class': 'off',
      },
    },
    {
      files: ['*.decorator.ts'],
      rules: {
        '@typescript-eslint/naming-convention': 'off',
      },
    },
  ],
};
