/** @type {import('prettier-plugin-embed').PrettierPluginEmbedOptions} */
const prettierPluginEmbedConfig = {
  embeddedSqlTags: ["sql"],
};

/** @type {import('prettier-plugin-sql').SqlBaseOptions} */
const prettierPluginSqlConfig = {
  language: "postgresql",
  keywordCase: "lower",
  database: "postgresql",
};

/** @type {import('prettier').Config} */
module.exports = {
  plugins: ["prettier-plugin-embed", "prettier-plugin-sql"],
  ...prettierPluginEmbedConfig,
  ...prettierPluginSqlConfig,
};
