CC      = gcc
CFLAGS  = -Wall -Wextra -g -I include

SRC_DIR  = src
TEST_DIR = test
BIN_DIR  = bin

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(SRCS:$(SRC_DIR)/%.c=$(BIN_DIR)/%.o)

TEST_SRCS = $(wildcard $(TEST_DIR)/test_*.c)
TEST_BINS = $(TEST_SRCS:$(TEST_DIR)/%.c=$(BIN_DIR)/%)

.PHONY: all test clean

all: $(TEST_BINS)

# link each test binary with all src objects
$(BIN_DIR)/test_%: $(TEST_DIR)/test_%.c $(OBJS) | $(BIN_DIR)
	$(CC) $(CFLAGS) -I $(TEST_DIR) $^ -o $@

# compile src files
$(BIN_DIR)/%.o: $(SRC_DIR)/%.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

test: $(TEST_BINS)
	@for t in $(TEST_BINS); do \
		echo "\n=== $$t ==="; \
		./$$t; \
	done

%: $(BIN_DIR)/test_%
	./$(BIN_DIR)/test_$@

clean:
	rm -rf $(BIN_DIR)

.PHONY: indent
indent: 
	clang-format -i **/*.[ch]