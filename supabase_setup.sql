-- PREDICTIVE FOOTBALL - Datenbank Setup
-- Diesen Code in Supabase SQL Editor einfügen und ausführen

-- 1. Wetten Tabelle
create table if not exists wetten (
  id uuid default gen_random_uuid() primary key,
  created_at timestamp with time zone default now(),
  liga text not null,
  heim text not null,
  ausw text not null,
  datum date not null,
  richtung text not null check (richtung in ('ueber', 'unter')),
  quote numeric(5,2) not null,
  quote_unter numeric(5,2),
  quote_ueber numeric(5,2),
  impl_unter numeric(5,3),
  einsatz numeric(8,2) not null,
  status text default 'offen' check (status in ('offen', 'gewonnen', 'verloren')),
  tore_heim integer,
  tore_ausw integer,
  gewinn numeric(8,2),
  bankroll_danach numeric(8,2)
);

-- 2. Bankroll Tabelle
create table if not exists bankroll (
  id uuid default gen_random_uuid() primary key,
  created_at timestamp with time zone default now(),
  betrag numeric(8,2) not null,
  aktion text not null,
  notiz text
);

-- Startkapital eintragen
insert into bankroll (betrag, aktion, notiz)
values (50.00, 'start', 'Startkapital Demo-Konto')
on conflict do nothing;

-- 3. Row Level Security deaktivieren (öffentliches Demo-Konto)
alter table wetten enable row level security;
alter table bankroll enable row level security;

create policy "Alle dürfen lesen" on wetten for select using (true);
create policy "Alle dürfen schreiben" on wetten for insert with check (true);
create policy "Alle dürfen updaten" on wetten for update using (true);
create policy "Alle dürfen lesen" on bankroll for select using (true);
create policy "Alle dürfen schreiben" on bankroll for insert with check (true);

-- 4. signale_cache Tabelle (für fixtures via fetch_to_supabase.py)
create table if not exists signale_cache (
  id uuid default gen_random_uuid() primary key,
  liga text unique not null,
  spiele jsonb not null default '[]',
  updated_at timestamptz default now()
);

alter table signale_cache enable row level security;
create policy "Alle dürfen lesen"     on signale_cache for select using (true);
create policy "Alle dürfen schreiben" on signale_cache for insert with check (true);
create policy "Alle dürfen updaten"   on signale_cache for update using (true);

-- Fertig!
select 'Datenbank erfolgreich eingerichtet!' as status;
