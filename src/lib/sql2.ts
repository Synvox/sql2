import { AsyncLocalStorage } from "node:async_hooks";

const placeholder = Symbol();

export type Interpolable = Statement | number | string | boolean | null;

export class Statement {
  readonly strings: (string | typeof placeholder)[] = [];
  readonly values: Interpolable[] = [];

  constructor(strings: ReadonlyArray<string>, values: Interpolable[]) {
    if (strings.length - 1 !== values.length)
      throw new Error(
        `Invalid number of values: strings: ${JSON.stringify(strings)},  values: ${JSON.stringify(values)}`,
      );

    let givenStrings: (string | typeof placeholder)[] = [...strings];
    let givenValues: Interpolable[] = [...values];

    while (true) {
      if (givenStrings.length === 0 && givenValues.length === 0) break;
      if (givenStrings.length > 0) this.strings.push(givenStrings.shift()!);
      if (givenValues.length > 0) {
        const value = givenValues.shift()!;

        if (value instanceof Statement) {
          this.strings.push(...value.strings);
          this.values.push(...value.values);
        } else {
          this.strings.push(placeholder);
          this.values.push(value);
        }
      }
    }
  }

  parameterize(index: number) {
    return `$${index}`;
  }

  compile() {
    let result = "";
    let index = 1;

    for (let i = 0; i < this.strings.length; i++) {
      if (this.strings[i] === placeholder) {
        result += this.parameterize(index++);
      } else {
        result += this.strings[i] as string;
      }
    }

    return result;
  }
}

export interface Client {
  exec(query: string): Promise<void>;
  query<T>(query: string, values: any[]): Promise<{ rows: T[] }>;
  transaction<T>(fn: (trx: Client) => Promise<T>): Promise<T>;
}

let rootClient: Client | null = null;
const providedClient = new AsyncLocalStorage<Client>();
export function setClient(client: Client) {
  rootClient = client;
}

export function provideClient<N extends Client, R>(
  client: N,
  fn: () => Promise<R>,
) {
  return providedClient.run(client, fn);
}

function getClient() {
  const storeClient = providedClient.getStore();
  if (storeClient) return storeClient;
  if (!rootClient) throw new Error("No root client set");
  return rootClient;
}

export function getSql<Camelize extends boolean = true>({
  camelize = true as Camelize,
}: {
  camelize?: Camelize;
} = {}): typeof sql {
  class CustomStatement extends Statement {
    private transformCases<T>(res: T) {
      return (camelize ? toCamelCase(res) : res) as Camelize extends true
        ? DeepCamelKeys<typeof res>
        : typeof res;
    }

    async first<T>() {
      const result = await getClient().query<T>(this.compile(), this.values);

      return this.transformCases(result.rows[0]);
    }

    async all<T>() {
      const result = await getClient().query<T>(this.compile(), this.values);

      return this.transformCases(result.rows);
    }

    async query<T>() {
      const result = await getClient().query<T>(this.compile(), this.values);

      return this.transformCases({ rows: result.rows });
    }

    async exec() {
      if (this.values.length > 0)
        throw new Error("Values are not supported for exec");
      await getClient().exec(this.compile());
    }
  }

  const sql = Object.assign(
    (strings: TemplateStringsArray, ...values: Interpolable[]) =>
      new CustomStatement(strings, values),
    {
      tx<T>(fn: (s: typeof sql) => Promise<T>): Promise<T> {
        return getClient().transaction<T>(async (trx) => {
          return await provideClient(trx, async () => {
            return await fn(getSql());
          });
        });
      },

      ref(value: string): Statement {
        return new Statement([`"${value.replace(/"/g, '""')}"`], []);
      },

      literal(value: any): Statement {
        return new Statement(["", ""], [value]);
      },

      join(statements: Statement[], separator = sql`,`): Statement {
        const nonEmptyStatements = statements.filter((stmt) => {
          return (
            stmt.strings.some(
              (s) => typeof s === "string" && s.trim().length > 0,
            ) || stmt.values.length > 0
          );
        });

        const returned = nonEmptyStatements.reduce(
          (returned, curr, index) => {
            if (index === 0) {
              returned = sql`${curr}`;
            } else {
              returned = sql`${returned}${separator}${curr}`;
            }
            return returned;
          },
          sql``,
        );

        return returned;
      },
    },
  );

  return sql;
}

type Simplify<T> = {
  [KeyType in keyof T]: T[KeyType];
} & {};

type SnakeToCamelCase<S extends string> = S extends `${infer P1}_${infer P2}`
  ? `${Lowercase<P1>}${Capitalize<SnakeToCamelCase<P2>>}`
  : S;

type DeepCamelKeys<T> = T extends readonly any[]
  ? { [I in keyof T]: DeepCamelKeys<T[I]> }
  : T extends object
    ? {
        [K in keyof T as K extends string
          ? SnakeToCamelCase<K>
          : K]: DeepCamelKeys<T[K]>;
      }
    : T;

function toCamelCase<T>(obj: T): Simplify<DeepCamelKeys<T>> {
  if (Array.isArray(obj)) {
    return obj.map((item) => toCamelCase(item)) as DeepCamelKeys<T>;
  } else if (obj !== null && typeof obj === "object") {
    const newObj: any = {};
    for (const key in obj) {
      const camelKey = key.replace(/_(.)/g, (_, letter) =>
        letter.toUpperCase(),
      );
      newObj[camelKey] = toCamelCase((obj as any)[key]);
    }
    return newObj as DeepCamelKeys<T>;
  }
  return obj as DeepCamelKeys<T>;
}
