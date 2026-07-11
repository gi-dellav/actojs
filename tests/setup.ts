if (!process.env.ACTOJS_VERBOSE) {
  const noop = () => {};
  console.error = noop;
}
