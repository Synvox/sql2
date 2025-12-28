import * as fsp from "node:fs/promises";
import {
  QueryableStatement,
  Statement,
  type Interpolable,
} from "../../sql2.ts";

export async function fsPlugin<T extends QueryableStatement>(
  sql: (strings: TemplateStringsArray, ...values: Interpolable[]) => T
) {
  const sqlScript = await fsp.readFile(
    new URL("./fs.sql", import.meta.url),
    "utf-8"
  );

  const strings = Object.assign([sqlScript] as ReadonlyArray<string>, {
    raw: [sqlScript],
  });

  await sql(strings).exec();
}
