#pragma once

#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace finsignal {

struct Trade {
  std::string account_id;
  std::string security_id;
  std::string trade_date;
  std::string side;
  double quantity = 0.0;
};

struct StartingPosition {
  std::string account_id;
  std::string security_id;
  std::string position_date;
  double quantity = 0.0;
};

struct ReportedPosition {
  std::string account_id;
  std::string security_id;
  std::string position_date;
  double reported_quantity = 0.0;
};

struct QualityFlag {
  std::string account_id;
  std::string security_id;
  std::string flag_date;
  std::string flag_type;
};

struct PositionRow {
  std::string account_id;
  std::string security_id;
  std::string position_date;
  double starting_quantity = 0.0;
  double cumulative_buy_quantity = 0.0;
  double cumulative_sell_quantity = 0.0;
  double expected_position = 0.0;
  std::optional<double> reported_position;
  std::optional<double> position_difference;
  std::string reconciliation_status;
  std::string break_reason_code;
};

class EventEngine {
 public:
  void set_valid_securities(const std::vector<std::string>& security_ids);
  void add_trade(Trade trade);
  void add_starting_position(StartingPosition position);
  void add_reported_position(ReportedPosition position);
  void add_quality_flag(QualityFlag flag);
  void clear();
  std::vector<PositionRow> reconstruct() const;

 private:
  struct DailyActivity {
    double buy_quantity = 0.0;
    double sell_quantity = 0.0;
  };

  std::vector<Trade> trades_;
  std::vector<StartingPosition> starting_positions_;
  std::vector<ReportedPosition> reported_positions_;
  std::vector<QualityFlag> quality_flags_;
  std::unordered_set<std::string> valid_security_ids_;
};

}  // namespace finsignal
