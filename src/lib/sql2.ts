const placeholder = Symbol();

export type Interpolable = Statement | number | string | boolean | null;

export class Statement {
  readonly strings: (string | typeof placeholder)[] = [];
  readonly values: Interpolable[] = [];

  constructor(strings: ReadonlyArray<string>, values: Interpolable[]) {
    if (strings.length - 1 !== values.length)
      throw new Error("Invalid number of values");

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

export function join(interpolables: Interpolable[], separator: Statement) {
  return new Statement(
    [
      "",
      ...interpolables.map((_, i, { length }) =>
        i + 1 === length ? "" : separator.compile()
      ),
    ],
    interpolables
  );
}

export abstract class QueryableStatement extends Statement {
  /**
   * Executes the statement as a query.
   * No parameters are provided.
   */
  abstract exec(): Promise<void>;
  /**
   * Executes the statement as a query.
   * Parameters are provided.
   */
  abstract query<T>(): Promise<{ rows: T[] }>;
}
