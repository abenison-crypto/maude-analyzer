/**
 * Comprehensive documentation for safety signal detection methods.
 * Used by help panels, tooltips, and method descriptions throughout the UI.
 */

import type { SignalMethod } from '../types/signals'

export interface MethodDocumentation {
  name: string
  shortDescription: string
  whenToUse: string
  howItWorks: string
  thresholds: {
    high: string
    elevated: string
    normal: string
  }
  interpretation: string
  recommendedLookback: string
  limitations: string
  detailExplanation: (details: Record<string, unknown>) => string
}

export const METHOD_DOCS: Record<SignalMethod, MethodDocumentation> = {
  zscore: {
    name: 'Z-Score',
    shortDescription: 'Statistical anomaly vs historical average',
    whenToUse:
      'Use Z-Score when you want to detect unusual spikes in reporting for a specific entity compared to its own historical baseline. Best for identifying sudden changes that deviate from normal patterns.',
    howItWorks:
      'Compares the most recent month\'s event count to the historical average, measured in standard deviations. A Z-score of 2 means the recent count is 2 standard deviations above the historical mean.',
    thresholds: {
      high: '> 2.0 standard deviations',
      elevated: '> 1.0 standard deviations',
      normal: '<= 1.0 standard deviations',
    },
    interpretation:
      'Higher values indicate the recent period is unusually elevated compared to this entity\'s own history. A Z-score of 2.5 means the recent count is very unusual (only ~1% chance of occurring randomly).',
    recommendedLookback: '24 months (minimum 12 for stable baseline)',
    limitations:
      'Requires sufficient historical data. Less meaningful for entities with highly variable reporting or very low event counts.',
    detailExplanation: (details) => {
      const avg = details.avg_monthly as number
      const latest = details.latest_month as number
      return `${latest} events this month (avg: ${Math.round(avg)})`
    },
  },

  prr: {
    name: 'Proportional Reporting Ratio (PRR)',
    shortDescription: 'Compares death proportion to other entities',
    whenToUse:
      'Use PRR when you want to compare an entity\'s proportion of death reports to all other entities in the database. Best for identifying entities with disproportionately high serious outcomes.',
    howItWorks:
      'Calculates the ratio of death proportion for this entity vs. all others. PRR = (deaths_entity / total_entity) / (deaths_others / total_others). A PRR of 2.0 means this entity has twice the proportion of deaths.',
    thresholds: {
      high: 'PRR >= 3.0 AND lower CI >= 1.0 AND deaths >= 3',
      elevated: 'PRR >= 2.0 AND lower CI >= 1.0 AND deaths >= 3',
      normal: 'PRR < 2.0 OR lower CI < 1.0',
    },
    interpretation:
      'A PRR of 2.5 means this entity has 2.5x the death proportion compared to others. The confidence interval must exclude 1 for statistical significance.',
    recommendedLookback: '12-24 months for statistical power',
    limitations:
      'Requires at least 3 death reports. Can be influenced by reporting patterns. Does not account for device complexity or patient risk factors.',
    detailExplanation: (details) => {
      const a = details.a as number
      const c = details.c as number
      return `${a} deaths vs ${c} others`
    },
  },

  ror: {
    name: 'Reporting Odds Ratio (ROR)',
    shortDescription: 'Odds ratio of deaths vs other outcomes',
    whenToUse:
      'Use ROR for a more conservative measure than PRR that accounts for the odds structure of the data. Often used in pharmacovigilance as a standard disproportionality measure.',
    howItWorks:
      'Calculates the odds ratio from a 2x2 contingency table: ROR = (a*d)/(b*c) where a=entity deaths, b=entity non-deaths, c=other deaths, d=other non-deaths.',
    thresholds: {
      high: 'ROR >= 3.0 AND lower CI >= 1.0',
      elevated: 'ROR >= 2.0 AND lower CI >= 1.0',
      normal: 'ROR < 2.0 OR lower CI < 1.0',
    },
    interpretation:
      'An ROR of 2.5 means the odds of death are 2.5x higher for this entity compared to others. Confidence intervals indicate statistical reliability.',
    recommendedLookback: '12-24 months for statistical power',
    limitations:
      'Similar to PRR limitations. The odds ratio interpretation is less intuitive than risk ratios.',
    detailExplanation: (details) => {
      const a = details.a as number
      const b = details.b as number
      return `Odds ${((a / b) * 100).toFixed(1)}% higher`
    },
  },

  ebgm: {
    name: 'Empirical Bayes Geometric Mean (EBGM)',
    shortDescription: 'FDA-style Bayesian shrinkage estimate',
    whenToUse:
      'Use EBGM for a statistically robust measure that accounts for sampling variability, especially useful when event counts are low. This is the method used by the FDA for signal detection.',
    howItWorks:
      'Compares observed deaths to expected deaths (under independence), with Bayesian shrinkage to account for small sample sizes. EBGM = (observed + 0.5) / (expected + 0.5). EB05 is the 5th percentile lower bound.',
    thresholds: {
      high: 'EBGM >= 3.0 AND EB05 >= 1.0',
      elevated: 'EBGM >= 2.0 AND EB05 >= 1.0',
      normal: 'EBGM < 2.0 OR EB05 < 1.0',
    },
    interpretation:
      'EBGM shows how many times more deaths were observed than expected. EB05 >= 1 means we\'re 95% confident the true ratio is at least 1 (i.e., more deaths than expected).',
    recommendedLookback: '12-24 months',
    limitations:
      'Simplified implementation; full FDA method uses more sophisticated Bayesian priors.',
    detailExplanation: (details) => {
      const observed = details.observed as number
      const expected = details.expected as number
      return `Obs: ${observed}, Exp: ${Math.round(expected)}`
    },
  },

  cusum: {
    name: 'CUSUM (Cumulative Sum)',
    shortDescription: 'Detects gradual drift over time',
    whenToUse:
      'Use CUSUM when you want to detect gradual increases that might not show up as sudden spikes. Best for identifying systematic changes in reporting patterns over time.',
    howItWorks:
      'Tracks the cumulative sum of deviations from the mean, with a slack parameter to reduce noise. When the cumulative sum exceeds a control limit (typically 3-5), it indicates a significant shift.',
    thresholds: {
      high: 'CUSUM > 5.0 (major drift)',
      elevated: 'CUSUM > 3.0 (control limit exceeded)',
      normal: 'CUSUM <= 3.0',
    },
    interpretation:
      'A CUSUM of 4.2 with limit 3.0 means the control limit was exceeded, indicating a sustained increase in reporting above the baseline.',
    recommendedLookback: '24+ months (needs history to detect drift)',
    limitations:
      'More sensitive to sustained changes than isolated spikes. May miss single-month anomalies.',
    detailExplanation: (details) => {
      const controlLimit = details.control_limit as number
      const mean = details.mean as number
      return `Limit: ${controlLimit} (avg: ${Math.round(mean)})`
    },
  },

  yoy: {
    name: 'Year-over-Year (YoY)',
    shortDescription: 'Compare same period across years',
    whenToUse:
      'Use YoY when you want to compare the same time period (e.g., Q1 2019 vs Q1 2018) to account for seasonal patterns in reporting.',
    howItWorks:
      'Calculates the percentage change between the current period and the same period in the previous year: ((current - prior) / prior) * 100.',
    thresholds: {
      high: '> 100% increase (doubled)',
      elevated: '> 50% increase',
      normal: '<= 50% change',
    },
    interpretation:
      'A YoY of +156% means reports increased by 156% compared to the same period last year (e.g., from 100 to 256 events).',
    recommendedLookback: 'Full prior year for baseline',
    limitations:
      'Requires data from the comparison period. May be affected by changes in reporting practices, not just actual safety changes.',
    detailExplanation: (details) => {
      const current = details.current_period as number
      const comparison = details.comparison_period as number
      return `${current} vs ${comparison} last year`
    },
  },

  pop: {
    name: 'Period-over-Period (PoP)',
    shortDescription: 'Custom range comparison',
    whenToUse:
      'Use PoP when you want to compare two custom time periods that don\'t align with yearly boundaries. Useful for before/after comparisons around specific dates.',
    howItWorks:
      'Same calculation as YoY but with user-defined periods. Computes percentage change between Period A (current) and Period B (comparison).',
    thresholds: {
      high: '> 100% increase',
      elevated: '> 50% increase',
      normal: '<= 50% change',
    },
    interpretation:
      'Positive values indicate Period A has more events than Period B. The percentage shows the magnitude of change.',
    recommendedLookback: 'User-defined periods of similar duration',
    limitations:
      'Periods should be of similar length for meaningful comparison. No adjustment for seasonality.',
    detailExplanation: (details) => {
      const current = details.current_period as number
      const comparison = details.comparison_period as number
      return `${current} vs ${comparison} prior`
    },
  },

  rolling: {
    name: 'Rolling Average',
    shortDescription: 'Compare to moving baseline',
    whenToUse:
      'Use Rolling Average when you want to compare the most recent month against a recent moving baseline, reducing the influence of old data.',
    howItWorks:
      'Calculates the average and standard deviation over a rolling window (e.g., last 3 months), then compares the current month to this baseline in terms of standard deviations.',
    thresholds: {
      high: '> 2 standard deviations above rolling average',
      elevated: '> 1 standard deviation above rolling average',
      normal: '<= 1 standard deviation',
    },
    interpretation:
      'A value of 2.1 means the current month is 2.1 standard deviations above the recent rolling average, indicating an unusual spike.',
    recommendedLookback: '12 months with 3-month window',
    limitations:
      'Sensitive to recent trends. A generally increasing baseline will make new spikes look smaller.',
    detailExplanation: (details) => {
      const latest = details.latest as number
      const avg = details.rolling_avg as number
      return `${latest} vs avg ${Math.round(avg)}`
    },
  },
}

/**
 * Decision guide for selecting signal detection methods.
 */
export const METHOD_SELECTION_GUIDE = {
  questions: [
    {
      question: 'What are you trying to detect?',
      options: [
        {
          answer: 'Sudden spikes in reporting',
          methods: ['zscore', 'rolling'] as SignalMethod[],
          explanation: 'Z-Score and Rolling Average are best for detecting sudden increases compared to historical patterns.',
        },
        {
          answer: 'Gradual increases over time',
          methods: ['cusum'] as SignalMethod[],
          explanation: 'CUSUM is designed to detect drift and sustained changes that accumulate over time.',
        },
        {
          answer: 'Higher death rates than similar devices',
          methods: ['prr', 'ror', 'ebgm'] as SignalMethod[],
          explanation: 'Disproportionality methods compare an entity\'s outcome profile to others in the database.',
        },
        {
          answer: 'Year-over-year changes',
          methods: ['yoy'] as SignalMethod[],
          explanation: 'YoY comparison accounts for seasonal patterns by comparing the same period across years.',
        },
      ],
    },
    {
      question: 'How much data do you have?',
      options: [
        {
          answer: 'Limited data (< 20 events)',
          methods: ['ebgm'] as SignalMethod[],
          explanation: 'EBGM uses Bayesian shrinkage to provide more reliable estimates with limited data.',
        },
        {
          answer: 'Moderate data (20-100 events)',
          methods: ['zscore', 'prr', 'ror'] as SignalMethod[],
          explanation: 'Most methods work well with moderate sample sizes.',
        },
        {
          answer: 'Substantial data (100+ events)',
          methods: ['zscore', 'prr', 'ror', 'cusum', 'rolling'] as SignalMethod[],
          explanation: 'All time-based methods are reliable with substantial data.',
        },
      ],
    },
  ],
  quickRecommendations: {
    initial: {
      methods: ['zscore', 'ebgm'] as SignalMethod[],
      reason: 'Good starting combination: Z-Score for time trends, EBGM for disproportionality',
    },
    comprehensive: {
      methods: ['zscore', 'prr', 'ror', 'ebgm', 'cusum'] as SignalMethod[],
      reason: 'Full analysis covering both time trends and disproportionality',
    },
    fdaStyle: {
      methods: ['ebgm'] as SignalMethod[],
      reason: 'FDA-standard Bayesian approach for signal detection',
    },
  },
}

/**
 * Threshold reference table for quick lookup.
 */
export const THRESHOLD_REFERENCE: Array<{
  method: SignalMethod
  metric: string
  highCondition: string
  elevatedCondition: string
}> = [
  { method: 'zscore', metric: 'Z-Score', highCondition: '> 2.0', elevatedCondition: '> 1.0' },
  { method: 'prr', metric: 'PRR', highCondition: '>= 3.0 + CI > 1', elevatedCondition: '>= 2.0 + CI > 1' },
  { method: 'ror', metric: 'ROR', highCondition: '>= 3.0 + CI > 1', elevatedCondition: '>= 2.0 + CI > 1' },
  { method: 'ebgm', metric: 'EBGM', highCondition: '>= 3.0 + EB05 > 1', elevatedCondition: '>= 2.0 + EB05 > 1' },
  { method: 'cusum', metric: 'CUSUM', highCondition: '> 5.0', elevatedCondition: '> 3.0' },
  { method: 'yoy', metric: 'YoY %', highCondition: '> +100%', elevatedCondition: '> +50%' },
  { method: 'pop', metric: 'PoP %', highCondition: '> +100%', elevatedCondition: '> +50%' },
  { method: 'rolling', metric: 'Std Dev', highCondition: '> 2.0', elevatedCondition: '> 1.0' },
]

/**
 * Get a formatted explanation for why a signal was triggered.
 */
export function getSignalTriggerExplanation(
  method: SignalMethod,
  value: number,
  lowerCi?: number | null
): string {
  const threshold = THRESHOLD_REFERENCE.find((t) => t.method === method)

  if (!threshold) return ''

  const isHigh = checkThreshold(method, value, lowerCi, 'high')
  const isElevated = checkThreshold(method, value, lowerCi, 'elevated')

  if (isHigh) {
    return `${threshold.metric} = ${formatValue(method, value)} (${threshold.highCondition} for High)`
  } else if (isElevated) {
    return `${threshold.metric} = ${formatValue(method, value)} (${threshold.elevatedCondition} for Elevated)`
  }

  return `${threshold.metric} = ${formatValue(method, value)}`
}

function checkThreshold(
  method: SignalMethod,
  value: number,
  lowerCi: number | null | undefined,
  level: 'high' | 'elevated'
): boolean {
  const thresholds = {
    zscore: { high: 2.0, elevated: 1.0 },
    prr: { high: 3.0, elevated: 2.0, requiresCi: true },
    ror: { high: 3.0, elevated: 2.0, requiresCi: true },
    ebgm: { high: 3.0, elevated: 2.0, requiresCi: true },
    cusum: { high: 5.0, elevated: 3.0 },
    yoy: { high: 100, elevated: 50 },
    pop: { high: 100, elevated: 50 },
    rolling: { high: 2.0, elevated: 1.0 },
  }

  const t = thresholds[method]
  const threshold = level === 'high' ? t.high : t.elevated

  if ('requiresCi' in t && t.requiresCi) {
    return value >= threshold && (lowerCi ?? 0) >= 1
  }

  return value > threshold
}

function formatValue(method: SignalMethod, value: number): string {
  if (method === 'yoy' || method === 'pop') {
    return `${value > 0 ? '+' : ''}${value}%`
  }
  return value.toFixed(2)
}
