CREATE OR REPLACE FUNCTION observe() RETURNS trigger AS $$
  if 'module_hashlib' in SD:
    hashlib = SD['module_hashlib']
  else:
    import hashlib
    SD['module_hashlib'] = hashlib
  if 'module_cPickle' in SD:
    cPickle = SD['module_cPickle']
  else:
    import cPickle
    SD['module_cPickle'] = cPickle

  query = TD['args'][0]
  observe_id = TD['args'][1]
  listen_channel = TD['args'][2]

  h = hashlib.sha1()
  for row in plpy.cursor(query):
    h.update(cPickle.dumps(row))
  new_digest = h.digest()

  # Note: this GD is shared across tables (unlike SD) but is still
  # session-local.
  key = 'digest_%s' % observe_id
  old_digest, GD[key] = GD.get(key), new_digest
  should_notify = False

  if old_digest is None:
    # plpy.warning('Observe %s: initial hash is %r' % (observe_id, new_digest))
    should_notify = True
  elif old_digest != new_digest:
    # plpy.warning('Observe %s: hash changed from %r to %r' %
    #              (observe_id, old_digest, new_digest))
    should_notify = True
  else:
    # plpy.warning('Observe %s: hash steady at %r' % (observe_id, new_digest))
    pass

  if should_notify:
    plpy.execute('NOTIFY ' + plpy.quote_ident(listen_channel))
$$ LANGUAGE plpythonu;
