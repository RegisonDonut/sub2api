// Clash Verge/Mihomo profile script. Attach this script to the subscription
// that owns the "OpenAI" policy group. It contains no subscription or secret.
function main(config, profileName) {
  const basePort = 17901;
  const sourceGroupName = "🤖 OpenAI";
  const selectorPrefix = "sub2api-openai-slot-";
  const listenerPrefix = "sub2api-openai-listener-";
  const proxies = Array.isArray(config.proxies) ? config.proxies : [];
  const proxyGroups = Array.isArray(config["proxy-groups"]) ? config["proxy-groups"] : [];
  const sourceGroup = proxyGroups.find((group) => group.name === sourceGroupName);
  if (!sourceGroup) return config;

  const proxyByName = new Map(proxies.map((proxy) => [proxy.name, proxy]));
  let candidates = Array.isArray(sourceGroup.proxies)
    ? sourceGroup.proxies.filter((name) => proxyByName.has(name))
    : [];
  if (sourceGroup["include-all"] || candidates.length === 0) {
    candidates = proxies.map((proxy) => proxy.name).filter(Boolean);
  }

  const excludedTypes = String(sourceGroup["exclude-type"] || "")
    .split(/[|,]/)
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  candidates = candidates.filter((name) => {
    const proxy = proxyByName.get(name);
    return proxy && !excludedTypes.includes(String(proxy.type || "").toLowerCase());
  });

  const includePattern = compilePattern(sourceGroup.filter);
  const excludePattern = compilePattern(sourceGroup["exclude-filter"]);
  candidates = candidates.filter(
    (name) =>
      (!includePattern || includePattern.test(name)) &&
      (!excludePattern || !excludePattern.test(name)),
  );
  if (candidates.length === 0) return config;

  const groups = proxyGroups.filter(
    (group) => !String(group.name || "").startsWith(selectorPrefix),
  );
  const listeners = (Array.isArray(config.listeners) ? config.listeners : []).filter(
    (listener) => !String(listener.name || "").startsWith(listenerPrefix),
  );
  const listenerPassword = String(config.secret || "");

  candidates.forEach((candidate, index) => {
    const slot = index + 1;
    const selectorName = selectorPrefix + slot;
    groups.push({
      name: selectorName,
      type: "select",
      proxies: candidates.slice(index).concat(candidates.slice(0, index)),
    });
    const listener = {
      name: listenerPrefix + slot,
      type: "http",
      port: basePort + index,
      listen: "0.0.0.0",
      proxy: selectorName,
    };
    if (listenerPassword) {
      listener.users = [{ username: "slot" + slot, password: listenerPassword }];
    }
    listeners.push(listener);
  });

  config["proxy-groups"] = groups;
  config.listeners = listeners;
  return config;
}

function compilePattern(value) {
  if (!value) return null;
  try {
    return new RegExp(String(value));
  } catch (_) {
    return null;
  }
}
