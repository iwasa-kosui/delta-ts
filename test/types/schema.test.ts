import { describe, it, expect } from "vitest";
import { parseSchema } from "../../src/types/schema.js";

describe("parseSchema", () => {
  it("should parse a simple struct schema", () => {
    const schemaString = JSON.stringify({
      type: "struct",
      fields: [
        { name: "id", type: "long", nullable: false, metadata: {} },
        { name: "name", type: "string", nullable: true, metadata: {} },
      ],
    });

    const schema = parseSchema(schemaString);
    expect(schema.type).toBe("struct");
    expect(schema.fields).toHaveLength(2);
    expect(schema.fields[0].name).toBe("id");
    expect(schema.fields[0].type).toBe("long");
    expect(schema.fields[0].nullable).toBe(false);
    expect(schema.fields[1].name).toBe("name");
    expect(schema.fields[1].type).toBe("string");
    expect(schema.fields[1].nullable).toBe(true);
  });

  it("should parse nested struct types", () => {
    const schemaString = JSON.stringify({
      type: "struct",
      fields: [
        {
          name: "address",
          type: {
            type: "struct",
            fields: [
              { name: "city", type: "string", nullable: true, metadata: {} },
              { name: "zip", type: "string", nullable: true, metadata: {} },
            ],
          },
          nullable: true,
          metadata: {},
        },
      ],
    });

    const schema = parseSchema(schemaString);
    const addressField = schema.fields[0];
    expect(addressField.name).toBe("address");
    expect(typeof addressField.type).toBe("object");
    if (typeof addressField.type === "object" && addressField.type.type === "struct") {
      expect(addressField.type.fields).toHaveLength(2);
      expect(addressField.type.fields[0].name).toBe("city");
    }
  });

  it("should parse array types", () => {
    const schemaString = JSON.stringify({
      type: "struct",
      fields: [
        {
          name: "tags",
          type: {
            type: "array",
            elementType: "string",
            containsNull: true,
          },
          nullable: true,
          metadata: {},
        },
      ],
    });

    const schema = parseSchema(schemaString);
    const tagsField = schema.fields[0];
    expect(typeof tagsField.type).toBe("object");
    if (typeof tagsField.type === "object" && tagsField.type.type === "array") {
      expect(tagsField.type.elementType).toBe("string");
      expect(tagsField.type.containsNull).toBe(true);
    }
  });

  it("should parse map types", () => {
    const schemaString = JSON.stringify({
      type: "struct",
      fields: [
        {
          name: "properties",
          type: {
            type: "map",
            keyType: "string",
            valueType: "integer",
            valueContainsNull: true,
          },
          nullable: true,
          metadata: {},
        },
      ],
    });

    const schema = parseSchema(schemaString);
    const propsField = schema.fields[0];
    expect(typeof propsField.type).toBe("object");
    if (typeof propsField.type === "object" && propsField.type.type === "map") {
      expect(propsField.type.keyType).toBe("string");
      expect(propsField.type.valueType).toBe("integer");
      expect(propsField.type.valueContainsNull).toBe(true);
    }
  });

  it("should throw on invalid schema JSON", () => {
    expect(() => parseSchema("not json")).toThrow();
  });

  it("should default metadata to empty object", () => {
    const schemaString = JSON.stringify({
      type: "struct",
      fields: [
        { name: "id", type: "long", nullable: false },
      ],
    });

    const schema = parseSchema(schemaString);
    expect(schema.fields[0].metadata).toEqual({});
  });
});
