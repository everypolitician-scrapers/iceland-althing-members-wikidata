#!/bin/env ruby
# encoding: utf-8
# frozen_string_literal: true

require 'csv'
require 'pry'
require 'scraperwiki'
require 'wikidata/fetcher'

WIKIDATA_SPARQL_URL = 'https://query.wikidata.org/sparql'

def sparql(query)
  result = RestClient.get WIKIDATA_SPARQL_URL, accept: 'text/csv', params: { query: query }
  CSV.parse(result, headers: true, header_converters: :symbol)
rescue RestClient::Exception => e
  raise "Wikidata query #{query} failed: #{e.message}"
end

memberships_query = <<EOQ
  SELECT DISTINCT ?item ?itemLabel ?start_date ?end_date ?constituency ?constituencyLabel ?party ?partyLabel
  WHERE {
    ?item p:P39 ?statement .
    ?statement ps:P39 wd:Q33083139 .
    OPTIONAL { ?statement pq:P580 ?start_date }
    OPTIONAL { ?statement pq:P582 ?end_date }
    OPTIONAL { ?statement pq:P768 ?constituency }
    OPTIONAL { ?statement pq:P4100 ?party }
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
  }
EOQ

data = sparql(memberships_query).map(&:to_h).map do |r|
  {
    id:         r[:item].split('/').last,
    name:       r[:itemlabel],
    start_date: r[:start_date].to_s[0..9],
    end_date:   r[:end_date].to_s[0..9],
    area:       r[:constituencylabel],
    area_id:    r[:constituency].split('/').last,
    party:      r[:partylabel],
    party_id:   r[:party].split('/').last,
    term:       2013,
  }
end
data.each { |mem| puts mem.reject { |_, v| v.to_s.empty? }.sort_by { |k, _| k }.to_h } if ENV['MORPH_DEBUG']

ScraperWiki.sqliteexecute('DROP TABLE data') rescue nil
ScraperWiki.save_sqlite(%i(id term start_date), data)
