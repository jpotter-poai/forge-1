import { useSyncExternalStore } from "react";

export interface CategoryColorOption {
  key: string;
  label: string;
  textClass: string;
  badgeClass: string;
  swatchClass: string;
}

export interface CategoryStyleOverride {
  icon?: string;
  colorKey?: string;
}

interface CategoryStyleDefinition {
  icon: string;
  colorKey: string;
  textClass: string;
  badgeClass: string;
}

const CATEGORY_STYLE_STORAGE_KEY = "forge-category-style-overrides-v1";
const CATEGORY_STYLE_EVENT = "forge-category-styles-changed";

const DEFAULT_CATEGORY_ICON: Record<string, string> = {
  IO: "⇄",
  Operator: "+",
  Combine: "⊕",
  Transform: "Δ",
  Statistics: "σ",
  Clustering: "⊙",
  Factorization: "⊗",
  Dimensionality: "ℝ",
  Visualization: "📈",
  Special: "★",
  Custom: "★",
};

export const CATEGORY_COLOR_OPTIONS: CategoryColorOption[] = [
  {
    key: "violet",
    label: "Violet",
    textClass: "text-violet-400",
    badgeClass: "bg-violet-900/50 text-violet-300",
    swatchClass: "bg-violet-400",
  },
  {
    key: "green",
    label: "Green",
    textClass: "text-green-400",
    badgeClass: "bg-green-900/50 text-green-300",
    swatchClass: "bg-green-400",
  },
  {
    key: "amber",
    label: "Amber",
    textClass: "text-amber-400",
    badgeClass: "bg-amber-900/50 text-amber-300",
    swatchClass: "bg-amber-400",
  },
  {
    key: "sky",
    label: "Sky",
    textClass: "text-sky-400",
    badgeClass: "bg-sky-900/50 text-sky-300",
    swatchClass: "bg-sky-400",
  },
  {
    key: "blue",
    label: "Blue",
    textClass: "text-blue-400",
    badgeClass: "bg-blue-900/50 text-blue-300",
    swatchClass: "bg-blue-400",
  },
  {
    key: "emerald",
    label: "Emerald",
    textClass: "text-emerald-400",
    badgeClass: "bg-emerald-900/50 text-emerald-300",
    swatchClass: "bg-emerald-400",
  },
  {
    key: "pink",
    label: "Pink",
    textClass: "text-pink-400",
    badgeClass: "bg-pink-900/50 text-pink-300",
    swatchClass: "bg-pink-400",
  },
  {
    key: "orange",
    label: "Orange",
    textClass: "text-orange-400",
    badgeClass: "bg-orange-900/50 text-orange-300",
    swatchClass: "bg-orange-400",
  },
  {
    key: "teal",
    label: "Teal",
    textClass: "text-teal-400",
    badgeClass: "bg-teal-900/50 text-teal-300",
    swatchClass: "bg-teal-400",
  },
  {
    key: "yellow",
    label: "Yellow",
    textClass: "text-yellow-400",
    badgeClass: "bg-yellow-900/50 text-yellow-300",
    swatchClass: "bg-yellow-400",
  },
  {
    key: "purple",
    label: "Purple",
    textClass: "text-purple-400",
    badgeClass: "bg-purple-900/50 text-purple-300",
    swatchClass: "bg-purple-400",
  },
  {
    key: "rose",
    label: "Rose",
    textClass: "text-rose-400",
    badgeClass: "bg-rose-900/50 text-rose-300",
    swatchClass: "bg-rose-400",
  },
  {
    key: "fuchsia",
    label: "Fuchsia",
    textClass: "text-fuchsia-400",
    badgeClass: "bg-fuchsia-900/50 text-fuchsia-300",
    swatchClass: "bg-fuchsia-400",
  },
  {
    key: "lime",
    label: "Lime",
    textClass: "text-lime-400",
    badgeClass: "bg-lime-900/50 text-lime-300",
    swatchClass: "bg-lime-400",
  },
  {
    key: "cyan",
    label: "Cyan",
    textClass: "text-cyan-400",
    badgeClass: "bg-cyan-900/50 text-cyan-300",
    swatchClass: "bg-cyan-400",
  },
  {
    key: "indigo",
    label: "Indigo",
    textClass: "text-indigo-400",
    badgeClass: "bg-indigo-900/50 text-indigo-300",
    swatchClass: "bg-indigo-400",
  },
  {
    key: "red",
    label: "Red",
    textClass: "text-red-400",
    badgeClass: "bg-red-900/50 text-red-300",
    swatchClass: "bg-red-400",
  },
];

const CATEGORY_COLOR_BY_KEY = new Map(
  CATEGORY_COLOR_OPTIONS.map((option) => [option.key, option]),
);

const DEFAULT_CATEGORY_COLOR_KEY: Record<string, string> = {
  IO: "violet",
  Operator: "green",
  Combine: "amber",
  Transform: "sky",
  Statistics: "blue",
  Clustering: "emerald",
  Visualization: "pink",
  Factorization: "orange",
  Dimensionality: "teal",
  Special: "yellow",
  Custom: "purple",
};

export const CATEGORY_ICON_OPTIONS = [
  "◈",
  "★",
  "✦",
  "⬡",
  "◇",
  "△",
  "◉",
  "⊕",
  "σ",
  "Δ",
];

function categoryHash(category: string): number {
  let hash = 0;
  for (let i = 0; i < category.length; i += 1) {
    hash = (hash * 31 + category.charCodeAt(i)) >>> 0;
  }
  return hash;
}

function defaultColorKey(category: string): string {
  const known = DEFAULT_CATEGORY_COLOR_KEY[category];
  if (known) {
    return known;
  }
  return CATEGORY_COLOR_OPTIONS[
    categoryHash(category) % CATEGORY_COLOR_OPTIONS.length
  ].key;
}

function defaultIcon(category: string): string {
  return DEFAULT_CATEGORY_ICON[category] ?? "◈";
}

function emitCategoryStyleChange(): void {
  if (typeof window === "undefined") {
    return;
  }
  window.dispatchEvent(new Event(CATEGORY_STYLE_EVENT));
}

function sanitizeIcon(icon: string | undefined): string | undefined {
  if (!icon) {
    return undefined;
  }
  const trimmed = icon.trim();
  if (!trimmed) {
    return undefined;
  }
  return Array.from(trimmed).slice(0, 2).join("");
}

function normalizeOverride(
  override: CategoryStyleOverride | undefined,
): CategoryStyleOverride | undefined {
  if (!override) {
    return undefined;
  }

  const icon = sanitizeIcon(override.icon);
  const colorKey =
    typeof override.colorKey === "string" &&
    CATEGORY_COLOR_BY_KEY.has(override.colorKey)
      ? override.colorKey
      : undefined;

  if (!icon && !colorKey) {
    return undefined;
  }

  return { icon, colorKey };
}

function normalizeOverrides(
  overrides: Record<string, CategoryStyleOverride>,
): Record<string, CategoryStyleOverride> {
  const normalizedEntries = Object.entries(overrides)
    .map(([category, override]) => [category, normalizeOverride(override)] as const)
    .filter(
      (entry): entry is readonly [string, CategoryStyleOverride] =>
        Boolean(entry[1]) && entry[0].trim().length > 0,
    )
    .sort((a, b) => a[0].localeCompare(b[0], undefined, { sensitivity: "base" }));

  return Object.fromEntries(normalizedEntries);
}

export function isBuiltInCategory(category: string): boolean {
  return Object.prototype.hasOwnProperty.call(DEFAULT_CATEGORY_ICON, category);
}

export function getCategoryStyleOverrides(): Record<string, CategoryStyleOverride> {
  if (typeof window === "undefined") {
    return {};
  }

  try {
    const raw = window.localStorage.getItem(CATEGORY_STYLE_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as Record<string, CategoryStyleOverride>;
    if (!parsed || typeof parsed !== "object") {
      return {};
    }
    return normalizeOverrides(parsed);
  } catch {
    return {};
  }
}

export function categoryColorOption(key: string): CategoryColorOption | undefined {
  return CATEGORY_COLOR_BY_KEY.get(key);
}

export function saveCategoryStyleOverrides(
  overrides: Record<string, CategoryStyleOverride>,
): void {
  if (typeof window === "undefined") {
    return;
  }

  const normalized = normalizeOverrides(overrides);
  try {
    if (Object.keys(normalized).length === 0) {
      window.localStorage.removeItem(CATEGORY_STYLE_STORAGE_KEY);
    } else {
      window.localStorage.setItem(
        CATEGORY_STYLE_STORAGE_KEY,
        JSON.stringify(normalized),
      );
    }
  } catch {
    return;
  }

  emitCategoryStyleChange();
}

export function serializeCategoryStyleOverrides(
  overrides: Record<string, CategoryStyleOverride>,
): string {
  return JSON.stringify(normalizeOverrides(overrides));
}

export function resolveCategoryStyle(category: string): CategoryStyleDefinition {
  return resolveCategoryStyleFromOverrides(category, getCategoryStyleOverrides());
}

export function resolveCategoryStyleFromOverrides(
  category: string,
  overrides: Record<string, CategoryStyleOverride>,
): CategoryStyleDefinition {
  const override = normalizeOverride(overrides[category]);
  const colorKey = override?.colorKey ?? defaultColorKey(category);
  const color = CATEGORY_COLOR_BY_KEY.get(colorKey) ?? CATEGORY_COLOR_OPTIONS[0];

  return {
    icon: override?.icon ?? defaultIcon(category),
    colorKey,
    textClass: color.textClass,
    badgeClass: color.badgeClass,
  };
}

export function categoryIcon(category: string): string {
  return resolveCategoryStyle(category).icon;
}

export function categoryTextClass(category: string): string {
  return resolveCategoryStyle(category).textClass;
}

export function categoryBadgeClass(category: string): string {
  return resolveCategoryStyle(category).badgeClass;
}

export function categoryColorKey(category: string): string {
  return resolveCategoryStyle(category).colorKey;
}

function subscribe(callback: () => void): () => void {
  if (typeof window === "undefined") {
    return () => undefined;
  }

  const handler = () => callback();
  const storageHandler = (event: StorageEvent) => {
    if (event.key === CATEGORY_STYLE_STORAGE_KEY) {
      callback();
    }
  };

  window.addEventListener(CATEGORY_STYLE_EVENT, handler);
  window.addEventListener("storage", storageHandler);

  return () => {
    window.removeEventListener(CATEGORY_STYLE_EVENT, handler);
    window.removeEventListener("storage", storageHandler);
  };
}

export function useCategoryStyleVersion(): string {
  return useSyncExternalStore(
    subscribe,
    () => serializeCategoryStyleOverrides(getCategoryStyleOverrides()),
    () => "{}", 
  );
}
