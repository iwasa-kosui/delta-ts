export type PrimitiveType =
  | "string"
  | "long"
  | "integer"
  | "short"
  | "byte"
  | "float"
  | "double"
  | "boolean"
  | "binary"
  | "date"
  | "timestamp"
  | "timestamp_ntz"
  | "decimal";

export interface StructField {
  name: string;
  type: DeltaDataType;
  nullable: boolean;
  metadata: Record<string, unknown>;
}

export interface StructType {
  type: "struct";
  fields: StructField[];
}

export interface ArrayType {
  type: "array";
  elementType: DeltaDataType;
  containsNull: boolean;
}

export interface MapType {
  type: "map";
  keyType: DeltaDataType;
  valueType: DeltaDataType;
  valueContainsNull: boolean;
}

export type DeltaDataType = PrimitiveType | StructType | ArrayType | MapType;

interface RawStructField {
  name: string;
  type: unknown;
  nullable: boolean;
  metadata: Record<string, unknown>;
}

interface RawStructType {
  type: "struct";
  fields: RawStructField[];
}

function parseDataType(raw: unknown): DeltaDataType {
  if (typeof raw === "string") {
    return raw as PrimitiveType;
  }

  if (typeof raw !== "object" || raw === null) {
    throw new Error(`Invalid data type: ${JSON.stringify(raw)}`);
  }

  const obj = raw as Record<string, unknown>;

  if (obj.type === "struct") {
    return parseStructType(raw as RawStructType);
  }

  if (obj.type === "array") {
    return {
      type: "array",
      elementType: parseDataType(obj.elementType),
      containsNull: obj.containsNull as boolean,
    };
  }

  if (obj.type === "map") {
    return {
      type: "map",
      keyType: parseDataType(obj.keyType),
      valueType: parseDataType(obj.valueType),
      valueContainsNull: obj.valueContainsNull as boolean,
    };
  }

  throw new Error(`Unknown data type: ${JSON.stringify(raw)}`);
}

function parseStructType(raw: RawStructType): StructType {
  return {
    type: "struct",
    fields: raw.fields.map((f) => ({
      name: f.name,
      type: parseDataType(f.type),
      nullable: f.nullable,
      metadata: f.metadata ?? {},
    })),
  };
}

export function parseSchema(schemaString: string): StructType {
  const raw = JSON.parse(schemaString) as RawStructType;
  return parseStructType(raw);
}
