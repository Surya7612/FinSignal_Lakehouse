#include "finsignal/event_engine.hpp"

#include <algorithm>
#include <cmath>
#include <map>
#include <set>
#include <tuple>
#include <unordered_map>

namespace finsignal {
namespace {

struct PairKey {
  std::string account_id;
  std::string security_id;
};

struct PairKeyLess {
  bool operator()(const PairKey& lhs, const PairKey& rhs) const {
    return std::tie(lhs.account_id, lhs.security_id) < std::tie(rhs.account_id, rhs.security_id);
  }
};

struct FlagSignals {
  bool split_adjustment_break = false;
  bool duplicate_trade = false;
  bool late_arriving_trade = false;
  bool missing_price = false;
};

struct DailyActivity {
  double buy_quantity = 0.0;
  double sell_quantity = 0.0;
};

bool IsBuySide(const std::string& side) {
  return side == "BUY" || side == "B";
}

bool IsSellSide(const std::string& side) {
  return side == "SELL" || side == "S";
}

bool DiffNonZero(const std::optional<double>& difference) {
  return difference.has_value() && std::abs(difference.value()) > 1e-12;
}

std::string ClassifyBreakReason(
    bool has_reported,
    const std::optional<double>& difference,
    const FlagSignals& flags) {
  if (!has_reported) {
    return "POSITION_NOT_REPORTED";
  }
  if (!DiffNonZero(difference)) {
    return "MATCH";
  }
  if (flags.split_adjustment_break) {
    return "SPLIT_ADJUSTMENT_BREAK";
  }
  if (flags.duplicate_trade) {
    return "DUPLICATE_TRADE";
  }
  if (flags.late_arriving_trade) {
    return "LATE_ARRIVING_TRADE";
  }
  if (flags.missing_price) {
    return "MISSING_PRICE";
  }
  return "QUANTITY_MISMATCH";
}

}  // namespace

void EventEngine::set_valid_securities(const std::vector<std::string>& security_ids) {
  valid_security_ids_.clear();
  valid_security_ids_.insert(security_ids.begin(), security_ids.end());
}

void EventEngine::add_trade(Trade trade) {
  trades_.push_back(std::move(trade));
}

void EventEngine::add_starting_position(StartingPosition position) {
  starting_positions_.push_back(std::move(position));
}

void EventEngine::add_reported_position(ReportedPosition position) {
  reported_positions_.push_back(std::move(position));
}

void EventEngine::add_quality_flag(QualityFlag flag) {
  quality_flags_.push_back(std::move(flag));
}

void EventEngine::clear() {
  trades_.clear();
  starting_positions_.clear();
  reported_positions_.clear();
  quality_flags_.clear();
  valid_security_ids_.clear();
}

std::vector<PositionRow> EventEngine::reconstruct() const {
  std::set<PairKey, PairKeyLess> pairs;
  std::set<std::string> all_dates;

  for (const auto& row : starting_positions_) {
    pairs.insert({row.account_id, row.security_id});
    all_dates.insert(row.position_date);
  }
  for (const auto& row : trades_) {
    pairs.insert({row.account_id, row.security_id});
    all_dates.insert(row.trade_date);
  }
  for (const auto& row : reported_positions_) {
    pairs.insert({row.account_id, row.security_id});
    all_dates.insert(row.position_date);
  }

  std::map<PairKey, double, PairKeyLess> starting_quantity_by_pair;
  for (const auto& row : starting_positions_) {
    const PairKey key{row.account_id, row.security_id};
    starting_quantity_by_pair[key] = std::max(starting_quantity_by_pair[key], row.quantity);
  }

  std::map<std::tuple<std::string, std::string, std::string>, DailyActivity> daily_activity;
  for (const auto& trade : trades_) {
    if (!valid_security_ids_.empty() && valid_security_ids_.count(trade.security_id) == 0) {
      continue;
    }
    auto activity_key = std::make_tuple(trade.account_id, trade.security_id, trade.trade_date);
    if (IsBuySide(trade.side)) {
      daily_activity[activity_key].buy_quantity += trade.quantity;
    } else if (IsSellSide(trade.side)) {
      daily_activity[activity_key].sell_quantity += trade.quantity;
    }
  }

  std::map<std::tuple<std::string, std::string, std::string>, double> reported_by_key;
  for (const auto& row : reported_positions_) {
    reported_by_key[std::make_tuple(row.account_id, row.security_id, row.position_date)] =
        row.reported_quantity;
  }

  std::map<std::tuple<std::string, std::string, std::string>, FlagSignals> flags_by_key;
  for (const auto& flag : quality_flags_) {
    auto flag_key = std::make_tuple(flag.account_id, flag.security_id, flag.flag_date);
    auto& signals = flags_by_key[flag_key];
    if (flag.flag_type == "SPLIT_ADJUSTMENT_BREAK") {
      signals.split_adjustment_break = true;
    } else if (flag.flag_type == "DUPLICATE_TRADE") {
      signals.duplicate_trade = true;
    } else if (flag.flag_type == "LATE_ARRIVING_TRADE") {
      signals.late_arriving_trade = true;
    } else if (flag.flag_type == "MISSING_PRICE") {
      signals.missing_price = true;
    }
  }

  std::vector<PositionRow> output;
  output.reserve(pairs.size() * all_dates.size());

  for (const auto& pair : pairs) {
    double cumulative_buy = 0.0;
    double cumulative_sell = 0.0;
    const double starting_quantity = starting_quantity_by_pair.count(pair) ? starting_quantity_by_pair.at(pair) : 0.0;

    for (const auto& position_date : all_dates) {
      const auto activity_key = std::make_tuple(pair.account_id, pair.security_id, position_date);
      if (daily_activity.count(activity_key) > 0) {
        cumulative_buy += daily_activity.at(activity_key).buy_quantity;
        cumulative_sell += daily_activity.at(activity_key).sell_quantity;
      }

      PositionRow row;
      row.account_id = pair.account_id;
      row.security_id = pair.security_id;
      row.position_date = position_date;
      row.starting_quantity = starting_quantity;
      row.cumulative_buy_quantity = cumulative_buy;
      row.cumulative_sell_quantity = cumulative_sell;
      row.expected_position = starting_quantity + cumulative_buy - cumulative_sell;

      const bool has_reported = reported_by_key.count(activity_key) > 0;
      if (has_reported) {
        row.reported_position = reported_by_key.at(activity_key);
        row.position_difference = row.reported_position.value() - row.expected_position;
      }

      const FlagSignals flags =
          flags_by_key.count(activity_key) > 0 ? flags_by_key.at(activity_key) : FlagSignals{};
      row.break_reason_code = ClassifyBreakReason(has_reported, row.position_difference, flags);

      if (!has_reported) {
        row.reconciliation_status = "BREAK";
      } else if (DiffNonZero(row.position_difference)) {
        row.reconciliation_status = "BREAK";
      } else {
        row.reconciliation_status = "MATCH";
      }

      output.push_back(std::move(row));
    }
  }

  std::sort(output.begin(), output.end(), [](const PositionRow& lhs, const PositionRow& rhs) {
    return std::tie(lhs.account_id, lhs.security_id, lhs.position_date) <
           std::tie(rhs.account_id, rhs.security_id, rhs.position_date);
  });

  return output;
}

}  // namespace finsignal
