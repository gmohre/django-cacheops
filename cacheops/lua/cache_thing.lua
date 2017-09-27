local key = KEYS[1]
local data = ARGV[1]
local dnfs = cjson.decode(ARGV[2])
local timeout = tonumber(ARGV[3])


-- Write data to cache
redis.call('setex', key, timeout, data)

local timeout_bump = timeout * 2 + 10

-- Update schemes and invalidators
for _, disj_pair in ipairs(dnfs) do

    local schemes_key = 'schemes:' .. disj_pair[1]
    local conj_key_prefix = 'conj:' .. disj_pair[1] .. ':'

    for _, conj in ipairs(disj_pair[2]) do

        local parts = {}
        local parts2 = {}
        for _, eq in ipairs(conj) do
            table.insert(parts, eq[1])
            table.insert(parts2, eq[1] .. '=' .. tostring(eq[2]))
        end
        local conj_schema = table.concat(parts, ',')
        local conj_key = conj_key_prefix .. table.concat(parts2, '&')

        -- Ensure scheme is known
        redis.call('sadd', schemes_key, conj_schema)

        -- Add new cache_key to list of dependencies
        redis.call('sadd', conj_key, key)
        -- NOTE: an invalidator should live longer than any key it references.
        --       So we update its ttl on every key if needed.
        -- NOTE: if CACHEOPS_LRU is True when invalidators should be left persistent,
        --       so we strip next section from this script.
        -- TOSTRIP
        if redis.call('ttl', conj_key) < timeout then
            -- We set conj_key life with a margin over key life to call expire rarer
            -- And add few extra seconds to be extra safe
            redis.call('expire', conj_key, timeout_bump)
        end
        -- /TOSTRIP
    end
end
