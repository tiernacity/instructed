#!/bin/bash
# CLI Test Suite for the Example Todo App
# Tests all CLI commands and validates output

PASS=0
FAIL=0
TOTAL=0
export PATH="$HOME:$PATH"
cd /workspace/example_todo

# Helper function
run_test() {
  TOTAL=$((TOTAL + 1))
  local test_name="$1"
  shift
  local expected="$1"
  shift
  
  echo -n "  Test $TOTAL: $test_name... "
  
  output=$(gleam run -- "$@" 2>&1 | grep -v "Compiled\|Running")
  
  if echo "$output" | grep -q "$expected"; then
    echo "PASS ✓"
    PASS=$((PASS + 1))
  else
    echo "FAIL ✗"
    echo "    Expected to contain: $expected"
    echo "    Got: $output"
    FAIL=$((FAIL + 1))
  fi
}

echo "═══════════════════════════════════"
echo " Todo CLI Test Suite"
echo "═══════════════════════════════════"
echo ""

# Setup - reset the event store
echo "Setup: Resetting event store..."
gleam run -- reset 2>&1 | grep -v "Compiled\|Running"
sleep 1
echo ""

echo "--- Basic CRUD Tests ---"

# Test 1: Add a todo with all fields
run_test "Add todo with all fields" "Todo created" add "Test todo 1" high 2026-03-01

# Test 2: Add todo with default priority
run_test "Add todo with default priority" "Todo created" add "Test todo 2"

# Test 3: Add todo with priority only
run_test "Add todo with priority" "Todo created" add "Test todo 3" critical

# Test 4: List all todos
sleep 1
run_test "List all shows todos" "All Todos" list

# Capture IDs from the list
sleep 1
ALL_OUTPUT=$(gleam run -- list 2>&1 | grep -v "Compiled\|Running")
ID1=$(echo "$ALL_OUTPUT" | grep "Test todo 1" | grep -o '\[[a-f0-9]*\]' | tr -d '[]')
ID2=$(echo "$ALL_OUTPUT" | grep "Test todo 2" | grep -o '\[[a-f0-9]*\]' | tr -d '[]')
ID3=$(echo "$ALL_OUTPUT" | grep "Test todo 3" | grep -o '\[[a-f0-9]*\]' | tr -d '[]')

echo ""
echo "  Captured IDs: ID1=$ID1, ID2=$ID2, ID3=$ID3"
echo ""

if [ -z "$ID1" ] || [ -z "$ID2" ] || [ -z "$ID3" ]; then
  echo "  ✗ Failed to capture todo IDs from listing. Aborting remaining tests."
  echo ""
  echo "═══════════════════════════════════"
  echo " Results: $PASS passed, $FAIL failed, $TOTAL total"
  echo "═══════════════════════════════════"
  exit 1
fi

echo "--- Command Validation Tests ---"

# Test 5: Complete a todo
run_test "Complete todo" "Todo completed" complete "$ID1"

# Test 6: Cannot complete already completed todo
run_test "Cannot complete completed todo" "already completed" complete "$ID1"

# Test 7: Reopen completed todo
run_test "Reopen todo" "Todo reopened" reopen "$ID1"

# Test 8: Cannot reopen active todo
run_test "Cannot reopen active todo" "already active" reopen "$ID1"

# Test 9: Complete again (after reopen)
run_test "Complete again after reopen" "Todo completed" complete "$ID1"

echo ""
echo "--- Edit Tests ---"

# Test 10: Edit description
run_test "Edit description" "Description updated" edit "$ID2" description "Updated description"

# Test 11: Edit priority
run_test "Edit priority" "Priority updated" edit "$ID2" priority critical

# Test 12: Edit due date
run_test "Edit due date" "Due date updated" edit "$ID2" due 2026-04-01

# Test 13: Invalid priority
run_test "Invalid priority rejected" "Invalid priority" edit "$ID2" priority invalid

echo ""
echo "--- Delete Tests ---"

# Test 14: Delete todo
run_test "Delete todo" "Todo deleted" delete "$ID3"

# Test 15: Cannot delete already deleted todo
run_test "Cannot delete deleted todo" "already deleted" delete "$ID3"

# Test 16: Cannot complete deleted todo
run_test "Cannot complete deleted todo" "deleted" complete "$ID3"

echo ""
echo "--- Projection/View Tests ---"

# Test 17: Active todos view
run_test "Active todos view" "Active Todos" list active

# Test 18: Completed todos view
run_test "Completed todos view" "Completed Todos" list completed

# Test 19: By priority view
run_test "By priority view" "by Priority" list by-priority

# Test 20: By due date view
run_test "By due date view" "by Due Date" list by-due-date

# Test 21: Overdue view
run_test "Overdue view" "Overdue Todos" list overdue

echo ""
echo "--- Help Test ---"

# Test 22: Help
run_test "Help output" "Todo CLI" help

echo ""
echo "--- Reset Test ---"

# Reset then list
gleam run -- reset 2>&1 | grep -v "Compiled\|Running" > /dev/null
sleep 1
run_test "Reset clears todos" "All Todos (0)" list

echo ""
echo "═══════════════════════════════════"
echo " Results: $PASS passed, $FAIL failed, $TOTAL total"
echo "═══════════════════════════════════"

if [ $FAIL -gt 0 ]; then
  exit 1
fi
