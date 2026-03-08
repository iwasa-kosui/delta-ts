export function versionToFilename(version: number): string {
  return `${version.toString().padStart(20, "0")}.json`;
}

export function versionToCheckpointFilename(version: number): string {
  return `${version.toString().padStart(20, "0")}.checkpoint.parquet`;
}

export function filenameToVersion(filename: string): number | null {
  const match = filename.match(/^(\d{20})\.json$/);
  if (!match) return null;
  return parseInt(match[1], 10);
}

export function deltaLogPath(tablePath: string): string {
  if (tablePath === "") return "_delta_log";
  return `${tablePath}/_delta_log`;
}
