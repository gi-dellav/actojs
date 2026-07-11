import { describe, test, expect } from "bun:test";
import { WebRuntime, setRuntime, getRuntime, hasMemoryAPI } from "../src/core";

describe("core", () => {
  describe("WebRuntime", () => {
    test('has name "web"', () => {
      const rt = new WebRuntime();
      expect(rt.name).toBe("web");
    });
  });

  describe("setRuntime / getRuntime", () => {
    test("default runtime is WebRuntime", () => {
      const rt = getRuntime();
      expect(rt.name).toBe("web");
    });

    test("can set a custom runtime", () => {
      const custom = { name: "custom" };
      const prev = getRuntime();
      setRuntime(custom);
      expect(getRuntime().name).toBe("custom");
      // Restore
      setRuntime(prev);
    });
  });

  describe("hasMemoryAPI", () => {
    test("returns boolean", () => {
      const result = hasMemoryAPI();
      expect(typeof result).toBe("boolean");
    });

    test("is consistent on multiple calls", () => {
      const a = hasMemoryAPI();
      const b = hasMemoryAPI();
      expect(a).toBe(b);
    });
  });
});
