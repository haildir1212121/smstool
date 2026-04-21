export function createClient(supabaseUrl, serviceKey) {
  const base = supabaseUrl.replace(/\/+$/, '') + '/rest/v1';
  const baseHeaders = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    'Content-Type': 'application/json',
  };

  async function query(table, { filters = {}, select = '*' } = {}) {
    const params = new URLSearchParams({ select });
    for (const [k, v] of Object.entries(filters)) {
      params.set(k, `eq.${v}`);
    }
    const res = await fetch(`${base}/${table}?${params}`, {
      headers: { ...baseHeaders, Prefer: 'return=representation' },
    });
    if (!res.ok) throw new Error(`Supabase query [${table}] failed: ${await res.text()}`);
    return res.json();
  }

  async function insert(table, row) {
    const res = await fetch(`${base}/${table}`, {
      method: 'POST',
      headers: { ...baseHeaders, Prefer: 'return=minimal' },
      body: JSON.stringify(row),
    });
    if (!res.ok) throw new Error(`Supabase insert [${table}] failed: ${await res.text()}`);
    return res.status === 204 ? null : res.json();
  }

  return { query, insert };
}
