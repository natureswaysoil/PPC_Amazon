#!/usr/bin/env python3
"""
Amazon PPC Automation Suite
===========================
Fixed & Updated for API V3 Reporting
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import gzip
import traceback

import requests

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml is required. Install with: pip install pyyaml")
    sys.exit(1)

# ============================================================================
# CONSTANTS
# ============================================================================

ENDPOINTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}

TOKEN_URL = "https://api.amazon.com/auth/o2/token"
USER_AGENT = "NWS-PPC-Automation/2.1"

# Rate limiting
MAX_REQUESTS_PER_SECOND = 5
REQUEST_INTERVAL = 1.0 / MAX_REQUESTS_PER_SECOND

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Auth:
    access_token: str
    token_type: str
    expires_at: float

    def is_expired(self) -> bool:
        return time.time() > self.expires_at - 60

@dataclass
class Campaign:
    campaign_id: str
    name: str
    state: str
    daily_budget: float
    targeting_type: str
    campaign_type: str = "sponsoredProducts"

@dataclass
class AdGroup:
    ad_group_id: str
    campaign_id: str
    name: str
    state: str
    default_bid: float

@dataclass
class Keyword:
    keyword_id: str
    ad_group_id: str
    campaign_id: str
    keyword_text: str
    match_type: str
    state: str
    bid: float

@dataclass
class PerformanceMetrics:
    impressions: int = 0
    clicks: int = 0
    cost: float = 0.0
    sales: float = 0.0
    orders: int = 0
    
    @property
    def ctr(self) -> float:
        return (self.clicks / self.impressions) if self.impressions > 0 else 0.0
    
    @property
    def acos(self) -> float:
        return (self.cost / self.sales) if self.sales > 0 else float('inf')
    
    @property
    def roas(self) -> float:
        return (self.sales / self.cost) if self.cost > 0 else 0.0

@dataclass
class AuditEntry:
    timestamp: str
    action_type: str
    entity_type: str
    entity_id: str
    old_value: str
    new_value: str
    reason: str
    dry_run: bool

# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    def __init__(self, max_per_second: int = MAX_REQUESTS_PER_SECOND):
        self.max_per_second = max_per_second
        self.interval = 1.0 / max_per_second
        self.last_request_time = 0.0
    
    def wait_if_needed(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.interval:
            sleep_time = self.interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

# ============================================================================
# CONFIGURATION LOADER
# ============================================================================

class Config:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.data = self._load_config()
    
    def _load_config(self) -> Dict:
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            # Return empty dict instead of exiting to allow safe fallback
            return {}
    
    def get(self, key: str, default=None):
        keys = key.split('.')
        value = self.data
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value if value is not None else default

# ============================================================================
# AUDIT LOGGER
# ============================================================================
class AuditLogger:
    """BigQuery-based audit logger"""
    
    def __init__(self, project_id, dataset_id, table_id):
        self.client = bigquery.Client(project=project_id)
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        self.entries = []
        
        # Ensure table exists (schema definition)
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("action_type", "STRING"),
            bigquery.SchemaField("entity_type", "STRING"),
            bigquery.SchemaField("entity_id", "STRING"),
            bigquery.SchemaField("old_value", "STRING"),
            bigquery.SchemaField("new_value", "STRING"),
            bigquery.SchemaField("reason", "STRING"),
            bigquery.SchemaField("dry_run", "BOOLEAN"),
        ]
        try:
            self.client.create_table(bigquery.Table(self.table_ref, schema=schema), exists_ok=True)
        except Exception as e:
            print(f"Table setup warning: {e}")

    def log(self, action_type, entity_type, entity_id, old_value, new_value, reason, dry_run=False):
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "action_type": action_type,
            "entity_type": entity_type,
            "entity_id": str(entity_id),
            "old_value": str(old_value),
            "new_value": str(new_value),
            "reason": str(reason),
            "dry_run": dry_run
        }
        self.entries.append(entry)

    def save(self):
        if not self.entries:
            return
        
        errors = self.client.insert_rows_json(self.table_ref, self.entries)
        if errors == []:
            print(f"Successfully uploaded {len(self.entries)} rows to BigQuery.")
        else:
            print(f"Encountered errors while inserting rows: {errors}")


# ============================================================================
# AMAZON ADS API CLIENT
# ============================================================================

class AmazonAdsAPI:
    def __init__(self, profile_id: str, region: str = "NA"):
        self.profile_id = profile_id
        self.region = region.upper()
        self.base_url = ENDPOINTS.get(self.region, ENDPOINTS["NA"])
        self.auth = self._authenticate()
        self.rate_limiter = RateLimiter()
    
    def _authenticate(self) -> Auth:
        client_id = os.getenv("AMAZON_CLIENT_ID")
        client_secret = os.getenv("AMAZON_CLIENT_SECRET")
        refresh_token = os.getenv("AMAZON_REFRESH_TOKEN")
        
        if not all([client_id, client_secret, refresh_token]):
            logger.error("Missing required environment variables for Auth")
            sys.exit(1)
        
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        
        try:
            response = requests.post(TOKEN_URL, data=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            return Auth(
                access_token=data["access_token"],
                token_type=data.get("token_type", "Bearer"),
                expires_at=time.time() + int(data.get("expires_in", 3600))
            )
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            sys.exit(1)
    
    def _refresh_auth_if_needed(self):
        if self.auth.is_expired():
            logger.info("Access token expired, refreshing...")
            self.auth = self._authenticate()
    
    def _headers(self) -> Dict[str, str]:
        self._refresh_auth_if_needed()
        return {
            "Authorization": f"{self.auth.token_type} {self.auth.access_token}",
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": os.getenv("AMAZON_CLIENT_ID"),
            "Amazon-Advertising-API-Scope": self.profile_id,
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }
    
    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        self.rate_limiter.wait_if_needed()
        url = f"{self.base_url}{endpoint}"
        max_retries = 3
        headers = self._headers()
        
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        
        for attempt in range(max_retries):
            try:
                response = requests.request(method=method, url=url, headers=headers, timeout=30, **kwargs)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 2))
                    logger.warning(f"Rate limit hit, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.HTTPError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Request failed: {e.response.text if e.response else e}")
                    raise
                time.sleep(2 * (attempt + 1))
        
        raise Exception("Max retries exceeded")

    # --- CAMPAIGNS ---
    def get_campaigns(self, state_filter: str = None) -> List[Campaign]:
        try:
            headers = {
                'Accept': 'application/vnd.spCampaign.v3+json',
                'Content-Type': 'application/vnd.spCampaign.v3+json'
            }
            payload = {}
            if state_filter:
                payload['stateFilter'] = {'include': [state_filter] if isinstance(state_filter, str) else state_filter}
            
            response = self._request('POST', '/sp/campaigns/list', json=payload, headers=headers)
            result = response.json()
            campaigns_data = result.get('campaigns', [])
            
            campaigns = []
            for c in campaigns_data:
                budget = c.get('budget', {}).get('budget', 0)
                campaign = Campaign(
                    campaign_id=str(c.get('campaignId')),
                    name=c.get('name', ''),
                    state=c.get('state', ''),
                    daily_budget=float(budget),
                    targeting_type=c.get('targetingType', ''),
                )
                campaigns.append(campaign)
            return campaigns
        except Exception as e:
            logger.error(f"Failed to get campaigns: {e}")
            return []

    def update_campaign(self, campaign_id: str, updates: Dict) -> bool:
        try:
            # V2 API fallback for simple updates is often safer, but V3 preferred
            # Using V2 endpoint for simplicity as it's still widely supported for simple state toggle
            response = self._request('PUT', f'/v2/sp/campaigns/{campaign_id}', json=updates)
            return True
        except Exception as e:
            logger.error(f"Failed to update campaign {campaign_id}: {e}")
            return False

    # --- AD GROUPS ---
    def get_ad_groups(self, campaign_id: str = None) -> List[AdGroup]:
        try:
            params = {}
            if campaign_id:
                params['campaignIdFilter'] = campaign_id
            response = self._request('GET', '/v2/sp/adGroups', params=params)
            ad_groups_data = response.json()
            
            ad_groups = []
            for ag in ad_groups_data:
                ad_groups.append(AdGroup(
                    ad_group_id=str(ag.get('adGroupId')),
                    campaign_id=str(ag.get('campaignId')),
                    name=ag.get('name', ''),
                    state=ag.get('state', ''),
                    default_bid=float(ag.get('defaultBid', 0))
                ))
            return ad_groups
        except Exception as e:
            logger.error(f"Failed to get ad groups: {e}")
            return []

    # --- KEYWORDS ---
    def get_keywords(self, campaign_id: str = None, ad_group_id: str = None) -> List[Keyword]:
        try:
            headers = {
                'Accept': 'application/vnd.spKeyword.v3+json',
                'Content-Type': 'application/vnd.spKeyword.v3+json'
            }
            payload = {}
            if campaign_id:
                payload['campaignIdFilter'] = {'include': [campaign_id]}
            if ad_group_id:
                payload['adGroupIdFilter'] = {'include': [ad_group_id]}
            
            response = self._request('POST', '/sp/keywords/list', json=payload, headers=headers)
            result = response.json()
            keywords_data = result.get('keywords', [])
            
            keywords = []
            for kw in keywords_data:
                keywords.append(Keyword(
                    keyword_id=str(kw.get('keywordId')),
                    ad_group_id=str(kw.get('adGroupId')),
                    campaign_id=str(kw.get('campaignId')),
                    keyword_text=kw.get('keywordText', ''),
                    match_type=kw.get('matchType', ''),
                    state=kw.get('state', ''),
                    bid=float(kw.get('bid', 0))
                ))
            return keywords
        except Exception as e:
            logger.error(f"Failed to get keywords: {e}")
            return []

    def update_keyword_bid(self, keyword_id: str, bid: float, state: str = None) -> bool:
        try:
            headers = {
                'Accept': 'application/vnd.spKeyword.v3+json',
                'Content-Type': 'application/vnd.spKeyword.v3+json'
            }
            updates = {
                'keywordId': str(keyword_id),
                'bid': round(bid, 2)
            }
            if state:
                updates['state'] = state
            
            response = self._request('PUT', '/sp/keywords', json={'keywords': [updates]}, headers=headers)
            logger.debug(f"Updated keyword {keyword_id} bid to ${bid:.2f}")
            return True
        except Exception as e:
            logger.error(f"Failed to update keyword {keyword_id}: {e}")
            return False
    
    def create_keywords(self, keywords_data: List[Dict]) -> List[str]:
        try:
            # Using V2 for creation as structure is simpler
            response = self._request('POST', '/v2/sp/keywords', json=keywords_data)
            result = response.json()
            created_ids = []
            for r in result:
                if r.get('code') == 'SUCCESS':
                    created_ids.append(str(r.get('keywordId')))
            return created_ids
        except Exception as e:
            logger.error(f"Failed to create keywords: {e}")
            return []

    # --- NEGATIVE KEYWORDS ---
    def get_negative_keywords(self, campaign_id: str = None) -> List[Dict]:
        try:
            params = {}
            if campaign_id:
                params['campaignIdFilter'] = campaign_id
            response = self._request('GET', '/v2/sp/negativeKeywords', params=params)
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get negative keywords: {e}")
            return []

    def create_negative_keywords(self, negative_keywords_data: List[Dict]) -> List[str]:
        try:
            response = self._request('POST', '/v2/sp/negativeKeywords', json=negative_keywords_data)
            result = response.json()
            created_ids = []
            for r in result:
                if r.get('code') == 'SUCCESS':
                    created_ids.append(str(r.get('keywordId')))
            return created_ids
        except Exception as e:
            logger.error(f"Failed to create negative keywords: {e}")
            return []

    # --- REPORTS (V3 IMPLEMENTATION) ---
    def create_report(self, report_type: str, metrics: List[str], 
                      report_date: str = None, segment: str = None) -> Optional[str]:
        """Create performance report using V3 Reporting API"""
        try:
            if report_date is None:
                report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                # Ensure date is YYYY-MM-DD for V3
                if len(report_date) == 8: # YYYYMMDD
                    report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:]}"

            # Map internal report types to V3 reportTypeIds
            type_map = {
                'keywords': 'spTargeting', # usually targeting is used for keyword performance
                'campaigns': 'spCampaigns',
                'targets': 'spSearchTerm' if segment == 'query' else 'spTargeting'
            }
            
            v3_report_type = type_map.get(report_type, 'spCampaigns')

            payload = {
                "startDate": report_date,
                "endDate": report_date,
                "configuration": {
                    "adProduct": "SP",
                    "groupBy": ["campaign", "adGroup", "targeting"] if v3_report_type == 'spTargeting' else ["campaign"],
                    "columns": metrics,
                    "reportTypeId": v3_report_type,
                    "format": "GZIP_JSON",
                    "timeUnit": "DAILY"
                }
            }
            
            # Adjust for Search Term report
            if v3_report_type == 'spSearchTerm':
                payload['configuration']['groupBy'] = ["campaign", "adGroup", "searchTerm"]
                payload['configuration']['reportTypeId'] = "spSearchTerm"

            headers = {
               "Content-Type": "application/vnd.createasyncreportrequest.v3+json"
            }

            response = self._request('POST', '/reporting/reports', json=payload, headers=headers)
            report_id = response.json().get('reportId')
            
            logger.info(f"Created report {report_id} (type: {v3_report_type})")
            return report_id
        except Exception as e:
            logger.error(f"Failed to create report: {e}")
            return None

    def get_report_status(self, report_id: str) -> Dict:
        try:
            response = self._request('GET', f'/reporting/reports/{report_id}')
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get report status: {e}")
            return {}

    def wait_for_report(self, report_id: str, timeout: int = 300) -> Optional[str]:
        start_time = time.time()
        while time.time() - start_time < timeout:
            status_data = self.get_report_status(report_id)
            status = status_data.get('status')
            
            if status == 'COMPLETED':
                return status_data.get('url')
            elif status in ['WZ_FAILED', 'FAILED', 'CANCELLED']:
                logger.error(f"Report {report_id} failed: {status}")
                return None
            
            time.sleep(5)
        logger.error(f"Report {report_id} timeout")
        return None

    def download_report(self, report_url: str) -> List[Dict]:
        try:
            response = requests.get(report_url, timeout=60)
            response.raise_for_status()
            
            # V3 reports are GZIP_JSON by default
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                    json_data = json.load(gz)
                    return json_data
            except Exception:
                 # Fallback if not gzipped or just raw json
                return json.loads(response.content)
                
        except Exception as e:
            logger.error(f"Failed to download report: {e}")
            return []

# ============================================================================
# AUTOMATION FEATURES (UNCHANGED LOGIC, JUST CONNECTED TO NEW API)
# ============================================================================

class BidOptimizer:
    def __init__(self, config: Config, api: AmazonAdsAPI, audit_logger: AuditLogger):
        self.config = config
        self.api = api
        self.audit = audit_logger
    
    def optimize(self, dry_run: bool = False) -> Dict:
        logger.info("=== Starting Bid Optimization ===")
        results = {'keywords_analyzed': 0, 'bids_increased': 0, 'bids_decreased': 0, 'no_change': 0}
        
        # V3 Metric names usually camelCase
        metrics_list = ['campaignId', 'adGroupId', 'keywordId', 'impressions', 'clicks', 'cost', 'attributedSales14d', 'attributedUnitsOrdered14d']
        
        report_id = self.api.create_report('keywords', metrics_list)
        
        if not report_id: return results
        report_url = self.api.wait_for_report(report_id)
        if not report_url: return results
        report_data = self.api.download_report(report_url)
        
        keywords = self.api.get_keywords()
        keyword_map = {kw.keyword_id: kw for kw in keywords}
        
        for row in report_data:
            keyword_id = str(row.get('keywordId'))
            if not keyword_id or keyword_id not in keyword_map: continue
            
            results['keywords_analyzed'] += 1
            keyword = keyword_map[keyword_id]
            
            metrics = PerformanceMetrics(
                impressions=int(row.get('impressions', 0)),
                clicks=int(row.get('clicks', 0)),
                cost=float(row.get('cost', 0.0)),
                sales=float(row.get('attributedSales14d', 0.0)),
                orders=int(row.get('attributedUnitsOrdered14d', 0))
            )
            
            new_bid = self._calculate_new_bid(keyword, metrics)
            
            if new_bid and abs(new_bid - keyword.bid) > 0.01:
                reason = self._get_bid_change_reason(keyword, metrics, new_bid)
                if new_bid > keyword.bid: results['bids_increased'] += 1
                else: results['bids_decreased'] += 1
                
                self.audit.log('BID_UPDATE', 'KEYWORD', keyword_id, f"${keyword.bid:.2f}", f"${new_bid:.2f}", reason, dry_run)
                if not dry_run: self.api.update_keyword_bid(keyword_id, new_bid)
            else:
                results['no_change'] += 1
        
        return results

    def _calculate_new_bid(self, keyword: Keyword, metrics: PerformanceMetrics) -> Optional[float]:
        min_clicks = self.config.get('bid_optimization.min_clicks', 25)
        min_spend = self.config.get('bid_optimization.min_spend', 5.0)
        high_acos = self.config.get('bid_optimization.high_acos', 0.60)
        low_acos = self.config.get('bid_optimization.low_acos', 0.25)
        up_pct = self.config.get('bid_optimization.up_pct', 0.15)
        down_pct = self.config.get('bid_optimization.down_pct', 0.20)
        min_bid = self.config.get('bid_optimization.min_bid', 0.25)
        max_bid = self.config.get('bid_optimization.max_bid', 5.0)
        
        if metrics.clicks < min_clicks and metrics.cost < min_spend: return None
        
        current_bid = keyword.bid
        new_bid = current_bid
        
        if metrics.sales <= 0 and metrics.clicks >= min_clicks:
            new_bid = current_bid * (1 - down_pct)
        elif metrics.acos > high_acos:
            new_bid = current_bid * (1 - down_pct)
        elif metrics.acos < low_acos and metrics.sales > 0:
            new_bid = current_bid * (1 + up_pct)
        else:
            return None
        
        new_bid = max(min_bid, min(max_bid, new_bid))
        return round(new_bid, 2)

    def _get_bid_change_reason(self, keyword: Keyword, metrics: PerformanceMetrics, new_bid: float) -> str:
        if metrics.sales <= 0: return f"No sales after {metrics.clicks} clicks"
        elif metrics.acos > self.config.get('bid_optimization.high_acos', 0.60): return f"High ACOS ({metrics.acos:.1%}) - reducing bid"
        elif metrics.acos < self.config.get('bid_optimization.low_acos', 0.25): return f"Low ACOS ({metrics.acos:.1%}) - increasing bid"
        else: return f"ACOS: {metrics.acos:.1%}, CTR: {metrics.ctr:.2%}"

class DaypartingManager:
    def __init__(self, config: Config, api: AmazonAdsAPI, audit_logger: AuditLogger):
        self.config = config
        self.api = api
        self.audit = audit_logger
        self.base_bids: Dict[str, float] = {}
    
    def apply_dayparting(self, dry_run: bool = False) -> Dict:
        logger.info("=== Applying Dayparting ===")
        if not self.config.get('dayparting.enabled', False):
            logger.info("Dayparting is disabled in config")
            return {}
        
        current_hour = datetime.now().hour
        current_day = datetime.now().strftime('%A').upper()
        multiplier = self._get_multiplier(current_hour, current_day)
        
        results = {'keywords_updated': 0, 'multiplier': multiplier}
        keywords = self.api.get_keywords()
        
        for keyword in keywords:
            if keyword.keyword_id not in self.base_bids:
                self.base_bids[keyword.keyword_id] = keyword.bid
            
            base_bid = self.base_bids[keyword.keyword_id]
            new_bid = base_bid * multiplier
            min_bid = self.config.get('bid_optimization.min_bid', 0.25)
            max_bid = self.config.get('bid_optimization.max_bid', 5.0)
            new_bid = round(max(min_bid, min(max_bid, new_bid)), 2)
            
            if abs(new_bid - keyword.bid) > 0.01:
                self.audit.log('DAYPARTING_ADJUSTMENT', 'KEYWORD', keyword.keyword_id, f"${keyword.bid:.2f}", f"${new_bid:.2f}", f"Time: {current_day} {current_hour}:00", dry_run)
                if not dry_run: self.api.update_keyword_bid(keyword.keyword_id, new_bid)
                results['keywords_updated'] += 1
        return results
    
    def _get_multiplier(self, hour: int, day: str) -> float:
        day_multipliers = self.config.get('dayparting.day_multipliers', {})
        day_multiplier = day_multipliers.get(day, 1.0)
        hour_multipliers = self.config.get('dayparting.hour_multipliers', {})
        hour_multiplier = hour_multipliers.get(hour, 1.0)
        return max(0.4, min(1.8, day_multiplier * hour_multiplier))

class CampaignManager:
    def __init__(self, config: Config, api: AmazonAdsAPI, audit_logger: AuditLogger):
        self.config = config
        self.api = api
        self.audit = audit_logger

    def manage_campaigns(self, dry_run: bool = False) -> Dict:
        logger.info("=== Managing Campaigns ===")
        results = {'campaigns_activated': 0, 'campaigns_paused': 0}
        
        report_id = self.api.create_report('campaigns', ['campaignId', 'cost', 'attributedSales14d'])
        if not report_id: return results
        report_url = self.api.wait_for_report(report_id)
        if not report_url: return results
        report_data = self.api.download_report(report_url)
        
        campaigns = self.api.get_campaigns()
        campaign_map = {c.campaign_id: c for c in campaigns}
        acos_threshold = self.config.get('campaign_management.acos_threshold', 0.45)
        
        for row in report_data:
            campaign_id = str(row.get('campaignId'))
            if campaign_id not in campaign_map: continue
            
            campaign = campaign_map[campaign_id]
            cost = float(row.get('cost', 0))
            sales = float(row.get('attributedSales14d', 0))
            if cost < 20.0: continue
            
            acos = (cost / sales) if sales > 0 else float('inf')
            
            if acos < acos_threshold and campaign.state != 'enabled':
                self.audit.log('CAMPAIGN_ACTIVATE', 'CAMPAIGN', campaign_id, campaign.state, 'enabled', f"ACOS {acos:.1%}", dry_run)
                if not dry_run: self.api.update_campaign(campaign_id, {'state': 'enabled'})
                results['campaigns_activated'] += 1
            elif acos > acos_threshold and campaign.state == 'enabled':
                self.audit.log('CAMPAIGN_PAUSE', 'CAMPAIGN', campaign_id, campaign.state, 'paused', f"ACOS {acos:.1%}", dry_run)
                if not dry_run: self.api.update_campaign(campaign_id, {'state': 'paused'})
                results['campaigns_paused'] += 1
                
        return results

# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================
class PPCAutomation:
    def __init__(self, config_path: str, profile_id: str, dry_run: bool = False):
        self.config = Config(config_path)
        self.profile_id = profile_id
        self.dry_run = dry_run
        region = self.config.get('api.region', 'NA')
        self.api = AmazonAdsAPI(profile_id, region)
        
        # -------------------------------------------------------
        # 👇 THIS IS THE UPDATE YOU NEED TO MAKE
        # -------------------------------------------------------
        self.audit = AuditLogger("natureswaysoil-video", "amazon_ppc", "audit_logs")
        # -------------------------------------------------------

        self.bid_optimizer = BidOptimizer(self.config, self.api, self.audit)
        self.dayparting = DaypartingManager(self.config, self.api, self.audit)
        self.campaign_manager = CampaignManager(self.config, self.api, self.audit)
        # (If you have keyword_discovery or negative_keywords classes, they go here too)

    def run(self, features: List[str] = None):
        logger.info(f"=== STARTING AUTOMATION (Profile: {self.profile_id}) ===")
        if features is None:
            features = self.config.get('features.enabled', [])
        
        results = {}
        try:
            if 'bid_optimization' in features:
                results['bid_optimization'] = self.bid_optimizer.optimize(self.dry_run)
            if 'dayparting' in features:
                results['dayparting'] = self.dayparting.apply_dayparting(self.dry_run)
            if 'campaign_management' in features:
                results['campaign_management'] = self.campaign_manager.manage_campaigns(self.dry_run)
        except Exception as e:
            logger.error(f"Automation failed: {e}")
            logger.error(traceback.format_exc())
        finally:
            # This saves the data to BigQuery
            self.audit.save()
        
        return results
class PPCAutomation:
    def __init__(self, config_path: str, profile_id: str, dry_run: bool = False):
        self.config = Config(config_path)
        self.profile_id = profile_id
        self.dry_run = dry_run
        region = self.config.get('api.region', 'NA')
        self.api = AmazonAdsAPI(profile_id, region)
        self.audit = AuditLogger()
        self.bid_optimizer = BidOptimizer(self.config, self.api, self.audit)
        self.dayparting = DaypartingManager(self.config, self.api, self.audit)
        self.campaign_manager = CampaignManager(self.config, self.api, self.audit)

    def run(self, features: List[str] = None):
        logger.info(f"=== STARTING AUTOMATION (Profile: {self.profile_id}) ===")
        if features is None:
            features = self.config.get('features.enabled', [])
        
        results = {}
        try:
            if 'bid_optimization' in features:
                results['bid_optimization'] = self.bid_optimizer.optimize(self.dry_run)
            if 'dayparting' in features:
                results['dayparting'] = self.dayparting.apply_dayparting(self.dry_run)
            if 'campaign_management' in features:
                results['campaign_management'] = self.campaign_manager.manage_campaigns(self.dry_run)
        except Exception as e:
            logger.error(f"Automation failed: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.audit.save()
        
        return results

# ============================================================================
# DASHBOARD WRAPPER (FIXED)
# ============================================================================
class PPCOptimizer:
    """Wrapper for dashboards."""

    def __init__(self, config_path: str = 'config.json'):
        self.config_path = config_path
        self.config = {}
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
        except Exception:
            pass
        
        # Ensure env vars are loaded for the API client
        if os.getenv('AMAZON_CLIENT_ID'):
            self.client_id = os.getenv('AMAZON_CLIENT_ID')
            self.profile_id = os.getenv('AMAZON_PROFILE_ID')
            self.region = os.getenv('AMAZON_REGION', 'NA')
        else:
            # Fallback to config file if env vars missing
            api_cfg = self.config.get('amazon_api', {})
            os.environ['AMAZON_CLIENT_ID'] = api_cfg.get('client_id', '')
            os.environ['AMAZON_CLIENT_SECRET'] = api_cfg.get('client_secret', '')
            os.environ['AMAZON_REFRESH_TOKEN'] = api_cfg.get('refresh_token', '')
            self.profile_id = api_cfg.get('profile_id')
            self.region = api_cfg.get('region', 'NA')

    def get_summary_metrics(self) -> dict:
        if self.profile_id and self.profile_id != 'YOUR_PROFILE_ID_HERE':
            try:
                # FIX: Instantiate correctly using profile_id and region
                # The class reads credentials from os.environ
                api = AmazonAdsAPI(self.profile_id, self.region)
                
                campaigns = api.get_campaigns()
                active_campaigns = [c for c in campaigns if c.state == 'enabled']
                
                return {
                    'total_campaigns': len(campaigns),
                    'active_campaigns': len(active_campaigns),
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'source': 'live_amazon_api'
                }
            except Exception as e:
                logger.error(f"Failed to get live metrics: {e}")
        
        return {
            'campaigns_checked': 0,
            'error': 'Missing credentials',
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Amazon PPC Automation Suite')
    parser.add_argument('--config', required=True, help='Path to configuration YAML file')
    parser.add_argument('--profile-id', required=True, help='Amazon Ads Profile ID')
    parser.add_argument('--dry-run', action='store_true', help='Run without making actual changes')
    parser.add_argument('--features', nargs='+', 
                        choices=['bid_optimization', 'dayparting', 'campaign_management'],
                        help='Specific features to run')
    
    args = parser.parse_args()
    automation = PPCAutomation(args.config, args.profile_id, args.dry_run)
    automation.run(args.features)

if __name__ == '__main__':
    main()





Evaluate

Compare
